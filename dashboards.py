from pyspark.sql import SparkSession
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import os
from matplotlib.backends.backend_pdf import PdfPages
import numpy as np

# Initialize Spark session for reading Delta tables
spark = SparkSession.builder \
    .appName("SchoolDistrictDW-Dashboards") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Set style and color palette
try:
    plt.style.use('seaborn-v0_8')
except OSError:
    plt.style.use('seaborn')
sns.set_palette("husl")
colors = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEAA7', '#DDA0DD', '#98D8C8', '#F7DC6F']

# Create reports directory
os.makedirs('reports', exist_ok=True)

base_path = "/tmp/delta/analytics"

# Helpers

def short_name(name: str) -> str:
    if not isinstance(name, str) or not name.strip():
        return ""
    parts = name.replace('&', ' ').split()
    return parts[-1] if parts else name

# Function to save both PNG and PDF
def save_plot(fig, filename, title):
    # Save PNG
    fig.savefig(f'reports/{filename}.png', dpi=300, bbox_inches='tight', facecolor='white')
    # Save PDF
    fig.savefig(f'reports/{filename}.pdf', bbox_inches='tight', facecolor='white')
    print(f"Saved: {filename}.png and {filename}.pdf")

# 1. Average grade per course - Horizontal Bar Chart
avg_grade_per_course = spark.read.format("delta").load(os.path.join(base_path, "avg_grade_per_course")).toPandas()
fig, ax = plt.subplots(figsize=(12, 8))
bars = ax.barh(avg_grade_per_course['course_name'], avg_grade_per_course['avg_gpa'], color=colors[:len(avg_grade_per_course)])
ax.set_xlabel('Average GPA', fontsize=12, fontweight='bold')
ax.set_ylabel('Course', fontsize=12, fontweight='bold')
ax.set_title('Average GPA per Course', fontsize=16, fontweight='bold', pad=20)
ax.grid(True, alpha=0.3)
# Add value labels on bars
for i, bar in enumerate(bars):
    width = bar.get_width()
    ax.text(width + 0.01, bar.get_y() + bar.get_height()/2, f'{width:.2f}', 
            ha='left', va='center', fontweight='bold')
plt.tight_layout()
save_plot(fig, 'avg_grade_per_course', 'Average GPA per Course')
plt.close()

# 2. Average grade per teacher - Vertical Bar Chart
avg_grade_per_teacher = spark.read.format("delta").load(os.path.join(base_path, "avg_grade_per_teacher")).toPandas()
fig, ax = plt.subplots(figsize=(14, 8))
bars = ax.bar(range(len(avg_grade_per_teacher)), avg_grade_per_teacher['avg_gpa'], 
              color=colors[:len(avg_grade_per_teacher)])
ax.set_xlabel('Teacher', fontsize=12, fontweight='bold')
ax.set_ylabel('Average GPA', fontsize=12, fontweight='bold')
ax.set_title('Average GPA per Teacher', fontsize=16, fontweight='bold', pad=20)
ax.set_xticks(range(len(avg_grade_per_teacher)))
ax.set_xticklabels(avg_grade_per_teacher['teacher_name'], rotation=45, ha='right')
ax.grid(True, alpha=0.3)
# Add value labels on bars
for i, bar in enumerate(bars):
    height = bar.get_height()
    ax.text(bar.get_x() + bar.get_width()/2, height + 0.01, f'{height:.2f}', 
            ha='center', va='bottom', fontweight='bold')
plt.tight_layout()
save_plot(fig, 'avg_grade_per_teacher', 'Average GPA per Teacher')
plt.close()

# 3. Homework submission rate per course - Horizontal Bar Chart
homework_submission_rate = spark.read.format("delta").load(os.path.join(base_path, "homework_submission_rate")).toPandas()
fig, ax = plt.subplots(figsize=(12, 8))
bars = ax.barh(homework_submission_rate['course_name'], homework_submission_rate['num_homeworks'], 
               color=colors[:len(homework_submission_rate)])
ax.set_xlabel('Number of Homework Submissions', fontsize=12, fontweight='bold')
ax.set_ylabel('Course', fontsize=12, fontweight='bold')
ax.set_title('Homework Submission Rate per Course', fontsize=16, fontweight='bold', pad=20)
ax.grid(True, alpha=0.3)
# Add value labels on bars
for i, bar in enumerate(bars):
    width = bar.get_width()
    ax.text(width + 0.5, bar.get_y() + bar.get_height()/2, f'{int(width)}', 
            ha='left', va='center', fontweight='bold')
plt.tight_layout()
save_plot(fig, 'homework_submission_rate', 'Homework Submission Rate per Course')
plt.close()

# 4. Parent engagement - Scatter Plot (with jitter and labels for all)
parent_engagement = spark.read.format("delta").load(os.path.join(base_path, "parent_engagement")).toPandas()
fig, ax = plt.subplots(figsize=(12, 8))
# Jitter to reduce overlap
np.random.seed(7)
pe = parent_engagement.copy()
# Coerce to float to avoid Decimal vs float errors
pe['num_children'] = pe['num_children'].astype(float)
pe['avg_gpa'] = pe['avg_gpa'].astype(float)
pe['num_children_j'] = pe['num_children'] + np.random.normal(0, 0.1, size=len(pe))
pe['avg_gpa_j'] = pe['avg_gpa'] + np.random.normal(0, 0.03, size=len(pe))
# Scatter with colormap
scatter = ax.scatter(pe['num_children_j'], pe['avg_gpa_j'], 
                    c=range(len(pe)), cmap='tab10', s=180, alpha=0.75, edgecolors='black')
ax.set_xlabel('Number of Children', fontsize=12, fontweight='bold')
ax.set_ylabel('Average GPA of Children', fontsize=12, fontweight='bold')
ax.set_title('Parent Engagement: Children Count vs Average GPA', fontsize=16, fontweight='bold', pad=20)
ax.grid(True, alpha=0.3)
# Annotate top parents by GPA and extremes by children count
top_gpa_idx = pe.sort_values('avg_gpa', ascending=False).head(5).index
extreme_children_idx = pd.concat([pe.sort_values('num_children').head(2), pe.sort_values('num_children', ascending=False).head(2)]).index
annot_idx = list(dict.fromkeys(list(top_gpa_idx) + list(extreme_children_idx)))
for i in annot_idx:
    row = pe.loc[i]
    ax.annotate(row['parent_name'], (row['num_children_j'], row['avg_gpa_j']), 
                xytext=(8, 6), textcoords='offset points', fontsize=9,
                bbox=dict(boxstyle='round,pad=0.25', fc='white', ec='#555555', alpha=0.8))
# Add abbreviated labels for all other points
for i, row in pe.drop(index=annot_idx, errors='ignore').iterrows():
    ax.annotate(short_name(row['parent_name']), (row['num_children_j'], row['avg_gpa_j']),
                xytext=(6, 4), textcoords='offset points', fontsize=7, color='#333333', alpha=0.85)
plt.tight_layout()
save_plot(fig, 'parent_engagement', 'Parent Engagement Analysis')
plt.close()

# 5. Course load changes by student - Horizontal Bar Chart
course_load_changes = spark.read.format("delta").load(os.path.join(base_path, "course_load_changes")).toPandas()
fig, ax = plt.subplots(figsize=(12, 10))
bars = ax.barh(course_load_changes['student_name'], course_load_changes['num_changes'], 
               color=colors[:len(course_load_changes)])
ax.set_xlabel('Number of Course Changes', fontsize=12, fontweight='bold')
ax.set_ylabel('Student', fontsize=12, fontweight='bold')
ax.set_title('Course Load Changes by Student', fontsize=16, fontweight='bold', pad=20)
ax.grid(True, alpha=0.3)
# Add value labels on bars
for i, bar in enumerate(bars):
    width = bar.get_width()
    ax.text(width + 0.05, bar.get_y() + bar.get_height()/2, f'{int(width)}', 
            ha='left', va='center', fontweight='bold')
plt.tight_layout()
save_plot(fig, 'course_load_changes', 'Course Load Changes by Student')
plt.close()

# 6. Grade distribution - Pie Chart
grade_distribution = spark.read.format("delta").load(os.path.join(base_path, "grade_distribution")).toPandas()
fig, ax = plt.subplots(figsize=(12, 8))
wedges, texts, autotexts = ax.pie(grade_distribution['count'], labels=grade_distribution['grade'], 
                                  autopct='%1.1f%%', colors=colors[:len(grade_distribution)],
                                  startangle=90, explode=[0.05] * len(grade_distribution))
ax.set_title('Grade Distribution Across All Assignments', fontsize=16, fontweight='bold', pad=20)
# Make percentage text bold
for autotext in autotexts:
    autotext.set_color('white')
    autotext.set_fontweight('bold')
plt.tight_layout()
save_plot(fig, 'grade_distribution', 'Grade Distribution')
plt.close()

# 7. School performance comparison - Grouped Bar Chart
school_performance = spark.read.format("delta").load(os.path.join(base_path, "school_performance")).toPandas()
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 8))

# Average GPA by school
bars1 = ax1.bar(school_performance['school_name'], school_performance['avg_gpa'], 
                color=colors[:len(school_performance)])
ax1.set_xlabel('School', fontsize=12, fontweight='bold')
ax1.set_ylabel('Average GPA', fontsize=12, fontweight='bold')
ax1.set_title('Average GPA by School', fontsize=14, fontweight='bold')
ax1.tick_params(axis='x', rotation=45)
ax1.grid(True, alpha=0.3)
# Add value labels
for i, bar in enumerate(bars1):
    height = bar.get_height()
    ax1.text(bar.get_x() + bar.get_width()/2, height + 0.01, f'{height:.2f}', 
             ha='center', va='bottom', fontweight='bold')

# Total students by school
bars2 = ax2.bar(school_performance['school_name'], school_performance['total_students'], 
                color=colors[:len(school_performance)])
ax2.set_xlabel('School', fontsize=12, fontweight='bold')
ax2.set_ylabel('Total Students', fontsize=12, fontweight='bold')
ax2.set_title('Total Students by School', fontsize=14, fontweight='bold')
ax2.tick_params(axis='x', rotation=45)
ax2.grid(True, alpha=0.3)
# Add value labels
for i, bar in enumerate(bars2):
    height = bar.get_height()
    ax2.text(bar.get_x() + bar.get_width()/2, height + 0.5, f'{int(height)}', 
             ha='center', va='bottom', fontweight='bold')

plt.suptitle('School Performance Comparison', fontsize=16, fontweight='bold')
plt.tight_layout()
save_plot(fig, 'school_performance', 'School Performance Comparison')
plt.close()

# 8. Subject performance - Horizontal Bar Chart
subject_performance = spark.read.format("delta").load(os.path.join(base_path, "subject_performance")).toPandas()
fig, ax = plt.subplots(figsize=(12, 8))
bars = ax.barh(subject_performance['subject'], subject_performance['avg_gpa'], 
               color=colors[:len(subject_performance)])
ax.set_xlabel('Average GPA', fontsize=12, fontweight='bold')
ax.set_ylabel('Subject', fontsize=12, fontweight='bold')
ax.set_title('Subject Performance Analysis', fontsize=16, fontweight='bold', pad=20)
ax.grid(True, alpha=0.3)
# Add value labels
for i, bar in enumerate(bars):
    width = bar.get_width()
    ax.text(width + 0.01, bar.get_y() + bar.get_height()/2, f'{width:.2f}', 
            ha='left', va='center', fontweight='bold')
plt.tight_layout()
save_plot(fig, 'subject_performance', 'Subject Performance Analysis')
plt.close()

# 9. Teacher workload - Scatter Plot (with jitter and labels for all)
teacher_workload = spark.read.format("delta").load(os.path.join(base_path, "teacher_workload")).toPandas()
fig, ax = plt.subplots(figsize=(14, 8))
# Ensure we have valid data for the scatter plot
teacher_workload_clean = teacher_workload.dropna(subset=['avg_gpa_given']).copy()
# Coerce to float to avoid Decimal vs float issues
for col in ['courses_taught', 'students_taught', 'avg_gpa_given', 'total_grades_given']:
    teacher_workload_clean[col] = teacher_workload_clean[col].astype(float)
# Jitter to reduce overlap
np.random.seed(13)
teacher_workload_clean['courses_taught_j'] = teacher_workload_clean['courses_taught'] + np.random.normal(0, 0.15, size=len(teacher_workload_clean))
teacher_workload_clean['students_taught_j'] = teacher_workload_clean['students_taught'] + np.random.normal(0, 1.5, size=len(teacher_workload_clean))
# Scale marker size for readability
sizes = np.clip(teacher_workload_clean['total_grades_given'] * 2, 50, 600)
scatter = ax.scatter(teacher_workload_clean['courses_taught_j'], teacher_workload_clean['students_taught_j'], 
                    c=teacher_workload_clean['avg_gpa_given'], s=sizes, 
                    cmap='viridis', alpha=0.75, edgecolors='black')
ax.set_xlabel('Number of Courses Taught', fontsize=12, fontweight='bold')
ax.set_ylabel('Number of Students Taught', fontsize=12, fontweight='bold')
ax.set_title('Teacher Workload Analysis', fontsize=16, fontweight='bold', pad=20)
ax.grid(True, alpha=0.3)

# Add colorbar
cbar = plt.colorbar(scatter, ax=ax)
cbar.set_label('Average GPA Given', fontsize=12, fontweight='bold')

# Annotate top teachers by students taught and by avg GPA given (styled boxes)
top_students_idx = teacher_workload_clean.sort_values('students_taught', ascending=False).head(6).index
top_gpa_idx = teacher_workload_clean.sort_values('avg_gpa_given', ascending=False).head(4).index
annot_idx = list(dict.fromkeys(list(top_students_idx) + list(top_gpa_idx)))
for i in annot_idx:
    row = teacher_workload_clean.loc[i]
    ax.annotate(row['teacher_name'], (row['courses_taught_j'], row['students_taught_j']), 
                xytext=(10, 8), textcoords='offset points', fontsize=9,
                bbox=dict(boxstyle='round,pad=0.25', fc='white', ec='#555555', alpha=0.85))
# Add abbreviated labels for all other teachers
for i, row in teacher_workload_clean.drop(index=annot_idx, errors='ignore').iterrows():
    ax.annotate(short_name(row['teacher_name']), (row['courses_taught_j'], row['students_taught_j']),
                xytext=(8, 6), textcoords='offset points', fontsize=7, color='#333333', alpha=0.85)
plt.tight_layout()
save_plot(fig, 'teacher_workload', 'Teacher Workload Analysis')
plt.close()

# 10. Content type analysis - Donut Chart
content_analysis = spark.read.format("delta").load(os.path.join(base_path, "content_analysis")).toPandas()
fig, ax = plt.subplots(figsize=(10, 8))
wedges, texts, autotexts = ax.pie(content_analysis['count'], labels=content_analysis['file_type'], 
                                  autopct='%1.1f%%', colors=colors[:len(content_analysis)],
                                  startangle=90, pctdistance=0.85)
# Create donut chart
centre_circle = plt.Circle((0,0), 0.70, fc='white')
fig.gca().add_artist(centre_circle)
ax.set_title('Content Type Distribution', fontsize=16, fontweight='bold', pad=20)
# Make percentage text bold
for autotext in autotexts:
    autotext.set_color('white')
    autotext.set_fontweight('bold')
plt.tight_layout()
save_plot(fig, 'content_analysis', 'Content Type Distribution')
plt.close()

print("All dashboards created and saved in PNG and PDF formats in the 'reports' directory.")