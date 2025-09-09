from pyspark.sql import SparkSession
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import os
import numpy as np
import re

spark = SparkSession.builder \
    .appName("SchoolDistrictDW-MLDashboards") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

plt.style.use('seaborn-v0_8')
sns.set_palette("deep")

base_path = "/tmp/delta/ml"
report_dir = "ml_reports"
os.makedirs(report_dir, exist_ok=True)

# Helper to save PNG and PDF
def save_plot(fig, filename):
    fig.savefig(os.path.join(report_dir, f"{filename}.png"), dpi=300, bbox_inches='tight', facecolor='white')
    fig.savefig(os.path.join(report_dir, f"{filename}.pdf"), bbox_inches='tight', facecolor='white')
    print(f"Saved: {filename}.png and {filename}.pdf")

# Load data
features = spark.read.format("delta").load(f"{base_path}/student_features").toPandas()
clusters = spark.read.format("delta").load(f"{base_path}/student_clusters").toPandas()
preds = spark.read.format("delta").load(f"{base_path}/dt_predictions").toPandas()
metrics = spark.read.format("delta").load(f"{base_path}/dt_metrics").toPandas()
cm = spark.read.format("delta").load(f"{base_path}/dt_confusion_matrix").toPandas()

# 1) Cluster composition: counts per cluster
fig, ax = plt.subplots(figsize=(10, 6))
sns.countplot(data=clusters, x='cluster', palette='viridis', ax=ax)
ax.set_title('KMeans Cluster Counts', fontsize=16, fontweight='bold')
ax.set_xlabel('Cluster ID (group of similar students)')
ax.set_ylabel('Number of Students')
ax.grid(True, axis='y', alpha=0.3)
for p in ax.patches:
    ax.annotate(int(p.get_height()), (p.get_x() + p.get_width()/2, p.get_height()),
                ha='center', va='bottom', fontsize=10)
fig.text(0.01, -0.02, 'Use to size target groups (e.g., interventions by student archetype).', fontsize=9)
save_plot(fig, 'kmeans_cluster_counts')
plt.close()

# 2) Cluster centers (approx): feature means per cluster
cluster_means = clusters.groupby('cluster')[['num_courses','num_homeworks','avg_gpa','num_changes']].mean().reset_index()
fig, ax = plt.subplots(figsize=(12, 7))
cluster_means_melt = cluster_means.melt(id_vars='cluster', var_name='feature', value_name='value')
sns.barplot(data=cluster_means_melt, x='feature', y='value', hue='cluster', ax=ax)
ax.set_title('Cluster Feature Means', fontsize=16, fontweight='bold')
ax.set_xlabel('Student Feature (engagement/performance/load)')
ax.set_ylabel('Average Value (standard units)')
ax.grid(True, axis='y', alpha=0.3)
plt.legend(title='Cluster')
fig.text(0.01, -0.02, 'Compare clusters to tailor supports (e.g., high workload + low GPA).', fontsize=9)
save_plot(fig, 'kmeans_cluster_means')
plt.close()

# 3) Scatter: avg GPA vs homeworks, colored by cluster
fig, ax = plt.subplots(figsize=(10, 6))
scatter = ax.scatter(clusters['num_homeworks'], clusters['avg_gpa'], c=clusters['cluster'], cmap='tab10', s=80, alpha=0.8, edgecolors='black')
ax.set_title('GPA vs Homework by Cluster', fontsize=16, fontweight='bold')
ax.set_xlabel('Homework Submissions (engagement proxy)')
ax.set_ylabel('Average GPA (0.0–4.0, performance)')
ax.grid(True, alpha=0.3)
cbar = plt.colorbar(scatter, ax=ax)
cbar.set_label('Cluster ID')
fig.text(0.01, -0.02, 'Identify clusters with high engagement but low GPA to prioritize tutoring.', fontsize=9)
save_plot(fig, 'kmeans_scatter_gpa_homeworks')
plt.close()

# 4) Decision Tree Metrics (accuracy/precision/recall)
fig, ax = plt.subplots(figsize=(9, 5))
metrics_long = metrics.melt(var_name='metric', value_name='value')
sns.barplot(data=metrics_long, x='metric', y='value', palette='Set2', ax=ax)
ax.set_title('Decision Tree Metrics', fontsize=16, fontweight='bold')
ax.set_xlabel('Evaluation Metric (Accuracy: overall; Precision: FP control; Recall: FN control)')
ax.set_ylabel('Score (0–1, higher is better)')
ax.set_ylim(0, 1)
for p in ax.patches:
    ax.annotate(f"{p.get_height():.2f}", (p.get_x() + p.get_width()/2, p.get_height()+0.02), ha='center')
ax.grid(True, axis='y', alpha=0.3)
fig.text(0.01, -0.02, 'Use Precision to avoid mislabeling low performers as high; use Recall to avoid missing students needing support.', fontsize=9)
save_plot(fig, 'dt_metrics')
plt.close()

# 5) Confusion Matrix heatmap
pivot = cm.pivot_table(index='label', columns='prediction', values='count', fill_value=0)
pivot = pivot.astype(int)
fig, ax = plt.subplots(figsize=(7, 6))
sns.heatmap(pivot, annot=True, fmt='d', cmap='Blues', ax=ax)
ax.set_title('Decision Tree Confusion Matrix', fontsize=16, fontweight='bold')
ax.set_xlabel('Predicted (1=High Performer, 0=Low Performer)')
ax.set_ylabel('Actual (1=High Performer, 0=Low Performer)')
fig.text(0.01, -0.02, 'Top-left/bottom-right are correct; off-diagonals show mistakes. Minimize FP for honors placement; minimize FN for interventions.', fontsize=9)
save_plot(fig, 'dt_confusion_matrix')
plt.close()

# 6) Decision Tree structure (text and diagram with feature names)
# Load debug string
try:
    raw_debug = spark.read.format("delta").load(f"{base_path}/dt_tree_debug").toPandas()['debug_string'].iloc[0]
except Exception:
    raw_debug = "Decision tree structure not available. Run ml_analytics.py first."

# Map feature indices to names (assembler order): 0=num_courses,1=num_homeworks,2=num_changes
feature_map = {0: 'num_courses', 1: 'num_homeworks', 2: 'num_changes'}

# Replace 'feature N' with friendly names
def replace_features(text: str) -> str:
    def repl(m):
        idx = int(m.group(1))
        return f"feature {feature_map.get(idx, idx)}"
    return re.sub(r"feature\s+(\d+)", repl, text)

tree_debug = replace_features(raw_debug)

# Save the raw debug string as a text-like figure (with feature names)
fig, ax = plt.subplots(figsize=(13, 10))
ax.axis('off')
ax.text(0.01, 0.99, 'Decision Tree (Debug String with Feature Names)', fontsize=16, fontweight='bold', va='top')
ax.text(0.01, 0.95, 'Features: num_courses, num_homeworks, num_changes (scaled) | Label: high_performer', fontsize=10, va='top')
ax.text(0.01, 0.92, 'Label mapping: prediction 1 = High Performer, 0 = Low Performer', fontsize=10, va='top', color='#333')
ax.text(0.01, 0.88, tree_debug, fontsize=9, family='monospace', va='top', wrap=True)
fig.text(0.01, -0.02, 'Explains split logic with feature names for auditability.', fontsize=9)
save_plot(fig, 'dt_tree_text')
plt.close()

# Simple parsed diagram (multi-level) using indentation depth
lines = [ln for ln in tree_debug.split('\n') if ln.strip()]

nodes = []
edges = []
stack = []
node_id = 0
for ln in lines:
    # Depth by leading spaces (Spark uses indentation)
    depth = len(ln) - len(ln.lstrip(' '))
    label = ln.strip()
    # Keep only first ~80 chars for label
    if len(label) > 80:
        label = label[:77] + '...'
    while stack and stack[-1][0] >= depth:
        stack.pop()
    nodes.append((node_id, depth, label))
    if stack:
        edges.append((stack[-1][1], node_id))
    stack.append((depth, node_id))
    node_id += 1

# Limit for readability
max_nodes = 30
nodes = nodes[:max_nodes]
edges = [(p, c) for (p, c) in edges if p < max_nodes and c < max_nodes]

# Compute positions
fig, ax = plt.subplots(figsize=(13, 10))
ax.axis('off')
if nodes:
    depths = sorted(set(d for (_, d, _) in nodes))
    # Normalize depths to levels 0..N
    level_map = {d: i for i, d in enumerate(sorted(depths))}
    level_nodes = {}
    for nid, d, label in nodes:
        lvl = level_map[d]
        level_nodes.setdefault(lvl, []).append((nid, label))
    xpos = {}
    ypos = {}
    for lvl, items in level_nodes.items():
        y = 1 - lvl * 0.15
        n = len(items)
        for i, (nid, label) in enumerate(items):
            x = (i + 1) / (n + 1)
            xpos[nid] = x
            ypos[nid] = y
            ax.text(x, y, label, fontsize=8, ha='center', va='center',
                    bbox=dict(boxstyle='round,pad=0.25', fc='white', ec='#333', alpha=0.9))
    for p, c in edges:
        if p in xpos and c in xpos:
            ax.plot([xpos[p], xpos[c]], [ypos[p], ypos[c]], color='#666')

ax.set_title('Decision Tree (Simplified Diagram with Feature Names)', fontsize=16, fontweight='bold')
fig.text(0.01, -0.02, 'Label mapping: 1=High Performer, 0=Low Performer. Visual guide to top levels; use text view for full details.', fontsize=9)
save_plot(fig, 'dt_tree_diagram')
plt.close()

print("ML dashboards created and saved in ml_reports (PNG and PDF).")
