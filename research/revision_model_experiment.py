"""
Can a trained model predict revision issue dates better than the current
group-median method? Same rows, same information, time-respecting split.

Sample = (monthly checkpoint T, revision pending at T, later issued).
Features use only what was known at T. Train on early checkpoints (labels
already known at the train cutoff), test on later ones.
"""
import duckdb, numpy as np, pandas as pd, lightgbm as lgb, sys
D = 'data'
MIN_SAMPLE = 15
con = duckdb.connect()
for n in ['list_document_forecast', 'documents', 'projects', 'origen_type', 'hours']:
    con.execute(f"CREATE VIEW {n} AS SELECT * FROM read_parquet('{D}/{n}.parquet')")
day = lambda c: f"TRY_CAST(substr(CAST({c} AS VARCHAR), 1, 10) AS DATE)"

con.execute(f"""
CREATE TABLE rev AS SELECT * FROM (
  SELECT f.id AS document_id, f.project AS project_id, f.specialty AS specialty_id, f.document_type AS document_type_id,
         f.hs_total, f.weight, r.rev AS revision, {day('d.created')} AS created_date,
         CASE r.rev WHEN 'A' THEN {day('f.rev_a')}  WHEN 'B' THEN {day('f.rev_b')}  ELSE {day('f.rev_0')}  END AS planned_date,
         CASE r.rev WHEN 'A' THEN {day('f.erev_a')} WHEN 'B' THEN {day('f.erev_b')} ELSE {day('f.erev_0')} END AS issued_date
  FROM list_document_forecast f CROSS JOIN (VALUES ('A'),('B'),('0')) r(rev)
  LEFT JOIN documents d ON d.id = f.id
  WHERE NOT EXISTS (SELECT 1 FROM origen_type o WHERE o.id = f.origen_type AND o.ignored_count)
) WHERE planned_date IS NOT NULL""")
con.execute(f"""CREATE TABLE proj AS SELECT id AS project_id, {day('start_date')} AS start_date,
                coalesce({day('end_date_original')}, {day('end_date')}) AS end_date FROM projects""")
con.execute(f"CREATE TABLE hrs AS SELECT document AS document_id, {day('date')} AS d, hours FROM hours WHERE {day('date')} IS NOT NULL")
lo, hi = con.execute("SELECT min(issued_date), max(issued_date) FROM rev").fetchone()
con.execute(f"CREATE TABLE tdates AS SELECT CAST(t AS DATE) AS t FROM generate_series(DATE '{lo}' + INTERVAL 60 DAY, DATE '{hi}', INTERVAL 14 DAY) g(t)")

# pending at T and eventually issued
con.execute("""
CREATE TABLE pend AS
SELECT d.t, r.*, greatest(d.t - r.planned_date, 0) AS days_overdue
FROM tdates d JOIN rev r ON r.issued_date > d.t AND (r.created_date IS NULL OR r.created_date <= d.t)""")
con.execute("CREATE TABLE hist AS SELECT project_id, specialty_id, document_type_id, revision, issued_date, issued_date - planned_date AS delay_days FROM rev WHERE issued_date IS NOT NULL")
def stats(name, cols):
    c = ', '.join(f'h.{x}' for x in cols)
    con.execute(f"""CREATE TABLE {name} AS SELECT d.t, {c}, count(*) n, quantile_cont(h.delay_days,0.5) med, quantile_cont(h.delay_days,0.8) p80
                    FROM tdates d JOIN hist h ON h.issued_date <= d.t GROUP BY d.t, {c}""")
stats('st1', ['specialty_id','document_type_id','revision']); stats('st2', ['specialty_id','revision']); stats('st3', ['revision'])
con.execute("""CREATE TABLE late AS SELECT d.t, h.revision, quantile_cont(h.delay_days,0.5) late_p50, quantile_cont(h.delay_days,0.8) late_p80
               FROM tdates d JOIN hist h ON h.issued_date <= d.t AND h.delay_days > 0 GROUP BY 1,2""")
# project state at T
con.execute("""CREATE TABLE pstate AS
SELECT d.t, r.project_id,
       count(*) FILTER (r.issued_date <= d.t) AS p_issued,
       count(*) FILTER (r.issued_date IS NULL OR r.issued_date > d.t) AS p_pending,
       count(*) FILTER ((r.issued_date IS NULL OR r.issued_date > d.t) AND r.planned_date < d.t) AS p_overdue,
       avg(r.issued_date - r.planned_date) FILTER (r.issued_date <= d.t) AS p_mean_delay,
       median(r.issued_date - r.planned_date) FILTER (r.issued_date <= d.t) AS p_med_delay
FROM tdates d JOIN rev r ON (r.created_date IS NULL OR r.created_date <= d.t) GROUP BY 1,2""")
con.execute("""CREATE TABLE hstate AS
SELECT p.t, p.document_id, sum(h.hours) AS doc_hours, sum(h.hours) FILTER (h.d > p.t - 28) AS doc_hours_28d, max(h.d) AS last_hour_date
FROM (SELECT DISTINCT t, document_id FROM pend) p JOIN hrs h ON h.document_id = p.document_id AND h.d <= p.t GROUP BY 1,2""")

df = con.execute(f"""
WITH q AS (
  SELECT p.*,
         greatest(coalesce(CASE WHEN s1.n>={MIN_SAMPLE} THEN s1.med END, CASE WHEN s2.n>={MIN_SAMPLE} THEN s2.med END, CASE WHEN s3.n>={MIN_SAMPLE} THEN s3.med END, 0),0) AS typical,
         greatest(coalesce(CASE WHEN s1.n>={MIN_SAMPLE} THEN s1.p80 END, CASE WHEN s2.n>={MIN_SAMPLE} THEN s2.p80 END, CASE WHEN s3.n>={MIN_SAMPLE} THEN s3.p80 END, 0),0) AS pessimistic
  FROM pend p
  LEFT JOIN st1 s1 ON s1.t=p.t AND s1.revision=p.revision AND s1.specialty_id IS NOT DISTINCT FROM p.specialty_id AND s1.document_type_id IS NOT DISTINCT FROM p.document_type_id
  LEFT JOIN st2 s2 ON s2.t=p.t AND s2.revision=p.revision AND s2.specialty_id IS NOT DISTINCT FROM p.specialty_id
  LEFT JOIN st3 s3 ON s3.t=p.t AND s3.revision=p.revision
), r AS (
  SELECT q.*,
    (SELECT count(*) FROM hist h WHERE h.issued_date<=q.t AND h.revision=q.revision AND h.delay_days>q.days_overdue) later_n,
    (SELECT quantile_cont(h.delay_days-q.days_overdue,0.5) FROM hist h WHERE h.issued_date<=q.t AND h.revision=q.revision AND h.delay_days>q.days_overdue) later_p50,
    (SELECT quantile_cont(h.delay_days-q.days_overdue,0.8) FROM hist h WHERE h.issued_date<=q.t AND h.revision=q.revision AND h.delay_days>q.days_overdue) later_p80
  FROM q
)
SELECT r.t, r.project_id, r.document_id, r.revision, r.specialty_id, r.document_type_id, r.hs_total, r.weight,
       r.issued_date - r.t AS y,                                   -- days from T until issue (target)
       greatest(r.planned_date, r.t) - r.t AS naive,
       CASE WHEN r.planned_date < r.t THEN ceil(CASE WHEN r.later_n>=5 THEN r.later_p50 ELSE coalesce(l.late_p50,0) END)
            ELSE greatest(r.planned_date + CAST(round(r.typical) AS INTEGER), r.t) - r.t END AS cur_p50,
       CASE WHEN r.planned_date < r.t THEN ceil(CASE WHEN r.later_n>=5 THEN r.later_p80 ELSE coalesce(l.late_p80,0) END)
            ELSE greatest(r.planned_date + CAST(round(r.pessimistic) AS INTEGER), r.t) - r.t END AS cur_p80,
       r.planned_date - r.t AS days_to_planned, r.days_overdue, r.t - r.created_date AS doc_age, r.typical, r.pessimistic,
       ps.p_issued, ps.p_pending, ps.p_overdue, ps.p_mean_delay, ps.p_med_delay,
       r.t - pr.start_date AS proj_age, pr.end_date - r.t AS days_to_proj_end,
       CASE WHEN pr.end_date > pr.start_date THEN (r.t - pr.start_date) * 1.0 / (pr.end_date - pr.start_date) END AS proj_frac,
       coalesce(hs.doc_hours,0) AS doc_hours, coalesce(hs.doc_hours_28d,0) AS doc_hours_28d, r.t - hs.last_hour_date AS days_since_hours,
       -- earlier revision of the same document, if issued by T
       (SELECT max(x.issued_date - x.planned_date) FROM rev x WHERE x.document_id=r.document_id AND x.revision='A' AND r.revision<>'A' AND x.issued_date<=r.t) AS rev_a_delay,
       (SELECT count(*) FROM rev x WHERE x.document_id=r.document_id AND x.revision<>r.revision AND x.issued_date<=r.t) AS other_revs_issued,
       r.issued_date
FROM r LEFT JOIN late l ON l.t=r.t AND l.revision=r.revision
LEFT JOIN pstate ps ON ps.t=r.t AND ps.project_id=r.project_id
LEFT JOIN proj pr ON pr.project_id=r.project_id
LEFT JOIN hstate hs ON hs.t=r.t AND hs.document_id=r.document_id
""").df()
df['t'] = pd.to_datetime(df['t']); df['issued_date'] = pd.to_datetime(df['issued_date'])
df['hours_ratio'] = df.doc_hours / df.hs_total.replace(0, np.nan)
for c in ['revision','specialty_id','document_type_id']: df[c] = df[c].fillna('NA').astype('category')
feats = ['revision','specialty_id','document_type_id','hs_total','weight','days_to_planned','days_overdue','doc_age','typical','pessimistic',
         'p_issued','p_pending','p_overdue','p_mean_delay','p_med_delay','proj_age','days_to_proj_end','proj_frac',
         'doc_hours','doc_hours_28d','days_since_hours','hours_ratio','rev_a_delay','other_revs_issued']
print(f"rows {len(df)}, revisions {df[['document_id','revision']].drop_duplicates().shape[0]}, projects {df.project_id.nunique()}, checkpoints {df.t.nunique()} ({df.t.min().date()} → {df.t.max().date()})")

GAP = int(sys.argv[1]) if len(sys.argv) > 1 else 0
def evaluate(cut):
    cut = pd.Timestamp(cut)
    # GAP: only train on checkpoints old enough for (almost) every outcome to be known, to limit censoring bias
    train = df[(df.t < cut - pd.Timedelta(days=GAP)) & (df.issued_date < cut)]
    test = df[df.t >= cut]
    if len(test) < 200 or len(train) < 500: return None
    params = dict(n_estimators=400, learning_rate=0.03, num_leaves=15, min_child_samples=40, subsample=0.8, subsample_freq=1, colsample_bytree=0.8, verbose=-1, random_state=7)
    m50 = lgb.LGBMRegressor(objective='quantile', alpha=0.5, **params).fit(train[feats], train.y)
    m80 = lgb.LGBMRegressor(objective='quantile', alpha=0.8, **params).fit(train[feats], train.y)
    test = test.assign(ml_p50=np.maximum(m50.predict(test[feats]), 0), ml_p80=np.maximum(m80.predict(test[feats]), 0))
    test = test.assign(ml_p80=np.maximum(test.ml_p80, test.ml_p50))
    mae = lambda c: (test.y - test[c]).abs().mean()
    med = lambda c: (test.y - test[c]).abs().median()
    row = dict(cutoff=str(cut.date()), train=len(train), test=len(test), test_projects=test.project_id.nunique(),
               mae_naive=mae('naive'), mae_current=mae('cur_p50'), mae_model=mae('ml_p50'),
               medae_naive=med('naive'), medae_current=med('cur_p50'), medae_model=med('ml_p50'),
               cov80_current=(test.y <= test.cur_p80).mean()*100, cov80_model=(test.y <= test.ml_p80).mean()*100,
               bias_current=(test.y - test.cur_p50).median(), bias_model=(test.y - test.ml_p50).median())
    # project finish = latest pending revision, per (project, checkpoint)
    g = test.groupby(['project_id','t']).agg(actual=('y','max'), naive=('naive','max'), cur=('cur_p50','max'), cur80=('cur_p80','max'), ml=('ml_p50','max'), ml80=('ml_p80','max'), n=('y','size'))
    row.update(proj_cp=len(g), pmae_naive=(g.actual-g.naive).abs().mean(), pmae_current=(g.actual-g.cur).abs().mean(), pmae_model=(g.actual-g.ml).abs().mean(),
               pmae_current80=(g.actual-g.cur80).abs().mean(), pmae_model80=(g.actual-g.ml80).abs().mean(),
               pcov80_current=(g.actual<=g.cur80).mean()*100, pcov80_model=(g.actual<=g.ml80).mean()*100)
    imp = pd.Series(m50.booster_.feature_importance(importance_type='gain'), index=feats).sort_values(ascending=False)
    return row, imp, test

rows, imps = [], []
for cut in ['2025-11-01','2026-01-01','2026-03-01','2026-05-01','2026-07-01']:
    r = evaluate(cut)
    if r: rows.append(r[0]); imps.append(r[1] / r[1].sum())
res = pd.DataFrame(rows).round(1)
pd.set_option('display.width', 250); pd.set_option('display.max_columns', 40)
print("\nREVISION LEVEL (days from checkpoint to issue; lower MAE is better)")
print(res[['cutoff','train','test','test_projects','mae_naive','mae_current','mae_model','medae_naive','medae_current','medae_model','bias_current','bias_model','cov80_current','cov80_model']].to_string(index=False))
print("\nPROJECT FINISH (latest pending revision per project and checkpoint)")
print(res[['cutoff','proj_cp','pmae_naive','pmae_current','pmae_model','pmae_current80','pmae_model80','pcov80_current','pcov80_model']].to_string(index=False))
print("\nFEATURE IMPORTANCE (share of gain, mean over cutoffs)")
print((pd.concat(imps, axis=1).mean(axis=1).sort_values(ascending=False).head(12) * 100).round(1).to_string())
