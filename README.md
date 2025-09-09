# School District Storage Backend

This project provides a robust storage backend for a school district system using Apache Delta Lake, PySpark, analytics dashboards, and ML insights.

## Why this project supports the district's advanced reporting goals
- Advanced reporting first: All data models, pipelines, and code are designed around analytics as the primary deliverable. Every entity (schools, courses, users, grades, homework, adjustments, content) is modeled to enable high‑quality joins and aggregations.
- Delta Lake foundation: ACID transactions, time travel, schema evolution, and reliable batch/stream compatibility ensure accurate, reproducible reports at any cadence.
- Analytics as tables: Classic analytics are materialized as Delta tables (/tmp/delta/analytics). This enables BI tools or downstream jobs to consume stable, queryable datasets without re-running transformations.
- Dashboards by default: Reproducible Python dashboards (reports/) convert analytics into decision-ready visualizations (PNG/PDF) for principals, teachers, and district leaders.
- ML-enhanced insights: Optional ML pipeline (ml_reports/) adds clustering and risk classification to complement descriptive analytics with actionable segmentation and early-warning signals.
- Extensible and auditable: Clear schemas, feature engineering, and labeled tree explanations (with feature names) support governance and stakeholder trust.
- One-command runs: main.py orchestrates end-to-end generation of both classic and ML dashboards so reporting can be automated and scheduled.

## How it supports a backend reporting database for KPI dashboards
- Performance:
  - Columnar storage (Delta/Parquet under the hood) and predicate pushdown for fast scans.
  - Materialized analytics tables reduce repeated computation and speed dashboard loads.
  - Vectorized Spark execution and caching options for hot datasets.
- Scalability:
  - Spark-based pipelines scale from a laptop to multi-node clusters without code changes.
  - Delta Lake supports partitioning and OPTIMIZE/ZORDER (add in deployment) for very large tables.
  - Stateless, idempotent jobs enable reliable re-runs and incremental expansion.
- Maintainability:
  - Clear separation: raw entities → analytics Delta tables → dashboards; each step testable.
  - Schemas and feature engineering centralized; reproducible one-command runs via main.py.
  - Readme documentation and per-folder READMEs (reports/, ml_reports/) clarify ownership and usage.
- Clarity of reporting:
  - Purpose-built analytics tables (avg grades, workloads, distributions, ML outputs) minimize complex joins in the dashboard layer.
  - Consistent naming and typed columns; ML visuals include explicit label mapping and feature names.
  - Both PNG and PDF artifacts for easy distribution, review, and archival.

## Dimensional Model / ER Proof of Concept
Because source data access is not yet available, we propose a pragmatic dimensional model oriented to mission‑critical KPIs. This POC maps to the seeded Delta tables and can evolve with real data.

- Grain assumptions (per fact):
  - Fact_Grade: one row per student per assignment (or per graded submission where assignment_id is available; in this POC, we use per student per course submission via grades table).
  - Fact_Homework: one row per homework submission (student, course, submission_id).
  - Fact_CourseLoadChange: one row per student course adjustment action.
  - Fact_ContentAccess (future): one row per content view/download (requires event capture).

- Dimensions (conformed where applicable):
  - Dim_Date: calendar attributes (date_key, date, week, month, quarter, year, is_holiday, etc.).
  - Dim_Student: student_key (surrogate), natural student_id, demographics (future), school_key.
  - Dim_Teacher: teacher_key, natural teacher_id, school_key.
  - Dim_Course: course_key, course_id, subject, level, school_key, teacher_key.
  - Dim_School: school_key, school_id, name, type, region.
  - Dim_Parent/Guardian: parent_key, parent_id (future joins to student via relationships).
  - Dim_Action: action_key, action_name (add, drop, switch, audit) for adjustments.
  - Dim_Content: content_key, file_type, content_type (lesson, quiz, video), course_key, teacher_key.

- Facts and measures:
  - Fact_Grade(student_key, course_key, date_key, grade_numeric, grade_letter, points_earned, points_possible).
  - Fact_Homework(student_key, course_key, date_key, submitted_flag=1, file_type).
  - Fact_CourseLoadChange(student_key, course_key, date_key, action_key, change_count=1).
  - Fact_ContentAccess(student_key, content_key, date_key, access_count=1, access_type) — future.

- Slowly Changing Dimensions (SCD) strategy:
  - Dim_School, Dim_Teacher, Dim_Student: SCD2 (effective_from, effective_to, is_current) if attributes can change and historical tracking is needed.
  - Other dims (Action, Content type): SCD1 (overwrite) is likely sufficient.

- Keys:
  - Surrogate integer keys for all dimensions (e.g., student_key) with natural keys retained for lineage.
  - Facts store surrogate keys for performance and conformance across reports.

- KPI coverage:
  - Achievement: avg GPA by school/subject/teacher/course (Fact_Grade + Dim_*).
  - Engagement: homework submission rate by student/course/school (Fact_Homework).
  - Stability/Placement: add/drop/switch counts by student/course/school (Fact_CourseLoadChange).
  - Resource usage: content mix and (future) access metrics (Dim_Content + Fact_ContentAccess).

- Mapping from current Delta POC tables:
  - users.role='student' → Dim_Student; users.role='teacher' → Dim_Teacher; schools → Dim_School; courses → Dim_Course; contents → Dim_Content; adjustments → Fact_CourseLoadChange; homeworks → Fact_Homework; grades → Fact_Grade.
  - Date keys can be derived from created_at/updated_at or submission dates; a generated Dim_Date can be added as a build step.

- Implementation path in this repo:
  - Stage (current): entity tables in /tmp/delta; analytics aggregates in /tmp/delta/analytics.
  - Next: create dimensionalized Delta tables (dim_*, fact_*) from entities; optionally partition facts by date or school.
  - Validate with dashboards; expose dim/fact tables to BI tools.

This model is intentionally simple and extensible, allowing you to onboard real attributes (demographics, program participation, accommodations) and new events (attendance, assessments, LMS clicks) without re‑architecting.

## Requirements
- Python 3.8+
- Java 11 (OpenJDK 11 is recommended and tested)
- pip or conda
- pandas, seaborn, matplotlib (for analytics and dashboards)

## Java Installation
If you do not have Java 11 installed, you can install it with:

**Ubuntu/Debian:**
```bash
sudo apt-get update
sudo apt-get install openjdk-11-jdk
```

**Fedora/RHEL:**
```bash
sudo dnf install java-11-openjdk
```

**macOS (with Homebrew):**
```bash
brew install openjdk@11
```

After installation, ensure Java 11 is the default:
```bash
java -version
```
You should see output indicating version 11.

## Setup Instructions

### 1. Create a Virtual Environment
```bash
python3 -m venv venv
source venv/bin/activate
```

### 2. Install Requirements
```bash
pip install -r requirements.txt
```

### 3. Set Environment Variable (IMPORTANT)
You must set the PYSPARK_SUBMIT_ARGS environment variable for Delta Lake to work properly. This needs to be done in every new terminal session:

```bash
export PYSPARK_SUBMIT_ARGS="--packages io.delta:delta-spark_2.13:4.0.0 pyspark-shell"
```

### 4. Generate Classic Analytics Reports
Run the full pipeline (populate data, compute analytics, make dashboards):
```bash
python main.py
```
Outputs:
- reports/ (PNG + PDF charts)
- The script prints a summary and displays a few sample dashboards.
See reports/README.md for an explanation of each chart.

### 5. Optional: Generate ML Reports
Enable ML pipeline (feature engineering + KMeans + Decision Tree) and ML dashboards:
```bash
RUN_ML=1 python main.py
```
Outputs:
- ml_reports/ (PNG + PDF charts), plus Delta outputs under /tmp/delta/ml
See ml_reports/README.md for an explanation of each ML chart.

### 6. Run Individual Scripts (Optional)
If you want to run scripts individually:

```bash
# Populate and seed data into Delta tables
export PYSPARK_SUBMIT_ARGS="--packages io.delta:delta-spark_2.13:4.0.0 pyspark-shell"
python storage.py

# Compute classic analytics (saves Delta tables under /tmp/delta/analytics)
python analytics.py

# Build classic dashboards (writes PNG/PDF into reports/)
python dashboards.py

# Compute ML analytics (saves Delta tables under /tmp/delta/ml)
python ml_analytics.py

# Build ML dashboards (writes PNG/PDF into ml_reports/)
python ml_dashboards.py
```

You can also use spark-submit instead of python if preferred (see earlier examples).

## Deep dive: Python modules

### storage.py
- Purpose: Define entity schemas and seed a realistic, interrelated dataset for development/POC.
- Inputs: None (generates synthetic data using Python + random).
- Outputs (Delta):
  - /tmp/delta/users, /schools, /courses, /contents, /homeworks, /grades, /adjustments
- Key logic:
  - Schemas share common audit columns (id, created_at, updated_at, is_active, metadata).
  - Generates 5 schools; 3 teachers, 6 students, 3 parents, 1 clerk per school.
  - 3 courses per teacher across diverse subjects/levels; 4 content items per course.
  - Each student enrolls in 3–5 courses; submits homework per enrollment.
  - Letter grades sampled with realistic distribution; comments aligned to grade.
  - Course-load adjustments (add/drop/switch/audit) created per student.
- Configuration:
  - Base path: /tmp/delta (change `base_path` if needed).
- Extension points:
  - Replace synthetic data with real ETL ingestion; add attendance/assessment events.
  - Introduce partitioning (by school_id/date) once volumes grow.

### analytics.py
- Purpose: Produce analysis-ready Delta tables to power dashboards and BI.
- Inputs (Delta): entities in /tmp/delta/*
- Outputs (Delta): /tmp/delta/analytics/
  - avg_grade_per_course, avg_grade_per_teacher, homework_submission_rate,
    parent_engagement, course_load_changes,
    grade_distribution, school_performance, subject_performance,
    teacher_workload, content_analysis
- Key logic:
  - Uses Spark SQL with explicit letter→GPA mapping.
  - Aggregations materialized to reduce dashboard compute time.
- Configuration:
  - Base path: /tmp/delta/analytics.
- Extension points:
  - Add new subject taxonomies, terms/years, or attendance-based metrics.

### dashboards.py
- Purpose: Turn analytics Delta tables into decision-ready visuals.
- Inputs (Delta): /tmp/delta/analytics/*
- Outputs (files): reports/*.png + reports/*.pdf
- Key logic:
  - Multiple chart types (bar, scatter, pie/donut), clear labels, value annotations.
  - Overlap mitigation (jittering, selective labeling) for dense plots.
- Configuration:
  - Report folder: ./reports (configurable in code).
- Extension points:
  - Add faceting by school/term; add accessibility color palettes or themes.

### ml_analytics.py
- Purpose: Build student features and run ML (KMeans clustering; Decision Tree classification).
- Inputs (Delta): entities in /tmp/delta/*
- Outputs (Delta): /tmp/delta/ml/
  - student_features, student_clusters, dt_predictions, dt_confusion_matrix, dt_metrics, dt_tree_debug
- Key logic:
  - Features: num_courses, num_homeworks, avg_gpa, num_changes.
  - KMeans on standardized features; cluster assignment per student.
  - Decision Tree predicting high performer (avg_gpa ≥ 3.0) with metrics and confusion matrix.
  - Saves full tree debug string for explainability.
- Configuration:
  - KMeans k=4; DT maxDepth=3 (tune as needed).
- Extension points:
  - Add richer features (attendance, assessments); introduce train/test split and calibration.

### ml_dashboards.py
- Purpose: Visualize ML outputs with operational guidance.
- Inputs (Delta): /tmp/delta/ml/*
- Outputs (files): ml_reports/*.png + ml_reports/*.pdf
- Key logic:
  - Cluster counts/means; GPA vs homework scatter by cluster.
  - DT metrics with precision/recall guidance; confusion matrix with goal-oriented notes.
  - Decision tree text with feature names and simplified diagram (1=High, 0=Low performer).
- Configuration:
  - Report folder: ./ml_reports (configurable in code).
- Extension points:
  - Add SHAP/feature importance, ROC curves, calibration plots, and fairness audits.

### main.py
- Purpose: Orchestrate end-to-end generation of reports; optionally include ML.
- Behavior:
  - Runs storage → analytics → dashboards; then lists outputs. If RUN_ML=1, also runs ML pipelines and lists ml_reports.
- Configuration:
  - Use environment variable RUN_ML=1 to enable ML.
- Extension points:
  - Integrate with a scheduler (cron/Airflow); add CLI args (output paths, toggles).

## Folder Contents
- reports/: Classic analytics dashboards (PNG + PDF). See reports/README.md for chart-by-chart details and decision guidance.
- ml_reports/: ML dashboards (PNG + PDF) including a labeled decision tree. See ml_reports/README.md for chart-by-chart details and guidance.

## Questions for the Client (to clarify requirements)
- Data scope & sources:
  - What systems will feed the reporting database (SIS, LMS, gradebook, SSO/IdP, data lake)?
  - What is the initial historical backfill (how many years) and refresh cadence (hourly/daily/weekly)?
  - Are there sensitive fields (PII/PHI) with masking or role-based redaction requirements?
- KPIs & definitions:
  - What are the mission-critical KPIs and their exact definitions (e.g., GPA calculation, attendance thresholds)?
  - At what grains should KPIs roll up (student, course, teacher, school, district, term, year)?
  - Any regulatory reports (state/federal) that must be matched exactly?
- Dimensional model:
  - Which dimensions require slowly changing history (student demographics, program participation)?
  - What unique IDs should be treated as natural keys per entity (student_id, staff_id, course_id)?
- Access & governance:
  - Who are the dashboard consumers (district leaders, principals, teachers, parents) and what are their permissions?
  - Do you need row-level security by school/teacher/class? How are roles mapped from the IdP?
- Performance SLAs:
  - Target dashboard load times and concurrency (e.g., <3s for 200 users at peak)?
  - Acceptable data freshness for each KPI (T+0 near-real-time vs T+1 daily batch)?
- Reliability & operations:
  - Expected uptime/window for maintenance; alerting/escalation paths for failures.
  - Backup/restore and time-travel retention policies.
- ML usage:
  - Which ML use cases are in-scope (early warning, placement, scheduling)?
  - What are acceptable tradeoffs between precision/recall; how will predictions be reviewed by staff?
- Roadmap & integration:
  - Which external BI tools (Power BI, Tableau, Looker) or APIs need access to analytic/fact tables?
  - Planned future events (attendance, assessments, behavior, content access) to incorporate next.

## Notes
- The scripts create Delta Lake tables and populate them with sample data in /tmp/delta.
- For production, configure Spark and Delta Lake to use your preferred storage backend (e.g., S3, HDFS).

## Troubleshooting
- Missing Delta Lake classes error: Ensure you have set the PYSPARK_SUBMIT_ARGS environment variable before running any script.
- Java errors: Ensure you are using Java 11 (not Java 17+ or Java 8). Check with `java -version`.
- New terminal session: Remember to run `export PYSPARK_SUBMIT_ARGS="--packages io.delta:delta-spark_2.13:4.0.0 pyspark-shell"` in every new terminal session.
- Memory warnings: The warnings about memory allocation are normal for this dataset size and can be ignored.
