# Machine Learning Reports

This folder contains ML-driven visualizations based on features engineered from student activity. Each chart is exported as both PNG and PDF.

Features used:
- num_courses: number of distinct courses a student appears in (from grades).
- num_homeworks: number of homework submissions (engagement proxy).
- avg_gpa: average GPA mapped from letter grades (0.0–4.0).
- num_changes: number of course load adjustments (add/drop/switch/audit).

## 1) KMeans Cluster Counts (`kmeans_cluster_counts`)
- What: Number of students in each cluster (K=4).
- Axes: X = Cluster ID, Y = Number of students.
- Use it to: Size target cohorts for interventions or enrichment.

## 2) Cluster Feature Means (`kmeans_cluster_means`)
- What: Average feature values per cluster.
- Axes: X = Feature, Y = Mean value (standard units).
- Use it to: Profile clusters (e.g., high engagement + low GPA) and tailor supports.

## 3) GPA vs Homework by Cluster (`kmeans_scatter_gpa_homeworks`)
- What: Each point = student; color = cluster.
- Axes: X = Homework submissions (engagement), Y = Average GPA (performance).
- Use it to: Identify high-engagement, low-performance students for tutoring; or low-engagement, high-performance for motivation strategies.

## 4) Decision Tree Metrics (`dt_metrics`)
- What: Classification scores for predicting high performers.
- Axes: X = Metric (Accuracy, Precision, Recall), Y = Score (0–1).
- Use it to:
  - Accuracy: overall correctness.
  - Precision: reduce false positives (avoid mislabeling low performers as high) – useful for honors placement.
  - Recall: reduce false negatives (avoid missing students needing help) – useful for intervention targeting.

## 5) Decision Tree Confusion Matrix (`dt_confusion_matrix`)
- What: Counts of predictions vs actual labels.
- Axes: Columns = Predicted (1=High Performer, 0=Low Performer); Rows = Actual.
- Use it to: Inspect tradeoffs – minimize FP for selective programs; minimize FN for support programs.

## 6) Decision Tree Text (`dt_tree_text`)
- What: Full decision tree in text form with feature names.
- Reading: Shows split thresholds on features (num_courses, num_homeworks, num_changes) and leaf predictions. 
- Use it to: Audit the model’s logic and explain decisions.

## 7) Decision Tree Diagram (`dt_tree_diagram`)
- What: Simplified tree visualization (top levels) with feature names.
- Notes: Caption includes label mapping: 1=High Performer, 0=Low Performer.
- Use it to: Quickly understand decision pathways; pair with the text view for full details.

---
Tips:
- Re-run `ml_analytics.py` if data changes to refresh clusters and the tree.
- Use ML views alongside classic analytics in `../reports/` for balanced decisions.
