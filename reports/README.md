# Classic Analytics Reports

This folder contains the core analytics visualizations generated from Delta tables. Each chart is exported as both PNG and PDF.

## 1) Average GPA per Course (`avg_grade_per_course`)
- What: Mean GPA for each course.
- Axes: X = Average GPA (0.0–4.0), Y = Course name.
- Use it to: Identify underperforming courses for curriculum review; prioritize coaching/resources.

## 2) Average GPA per Teacher (`avg_grade_per_teacher`)
- What: Mean GPA across courses taught by each teacher.
- Axes: X = Teacher, Y = Average GPA.
- Use it to: Spot classes with unusual grading/performance patterns; plan peer mentoring or PD.

## 3) Homework Submission Rate per Course (`homework_submission_rate`)
- What: Count of homework submissions per course.
- Axes: X = Submission count, Y = Course.
- Use it to: Flag low-engagement courses and intervene (reminders, scaffolds, parent outreach).

## 4) Parent Engagement (`parent_engagement`)
- What: Each point = a parent. Shows number of children in the school and their average GPA.
- Axes: X = Number of children (per parent), Y = Average GPA of the children.
- Use it to: Identify families needing support and those to involve in peer-support networks.

## 5) Course Load Changes by Student (`course_load_changes`)
- What: Number of add/drop/switch/audit actions per student.
- Axes: X = Number of changes, Y = Student.
- Use it to: Detect instability or mismatch in course placement; follow up with counseling.

## 6) Grade Distribution (`grade_distribution`)
- What: Overall distribution of letter grades across assignments.
- Reading: Each slice shows the share of a letter grade.
- Use it to: Detect skew towards very high/low grades; calibrate assessments.

## 7) School Performance (`school_performance`)
- What: Side-by-side charts: average GPA and total students by school.
- Axes: Left: School vs Avg GPA; Right: School vs Student count.
- Use it to: Compare schools on performance and scale to allocate district resources.

## 8) Subject Performance (`subject_performance`)
- What: Average GPA by subject (aggregated from course names).
- Axes: X = Average GPA, Y = Subject.
- Use it to: Identify subjects needing curriculum or staffing support.

## 9) Teacher Workload (`teacher_workload`)
- What: Each point = a teacher; color = average GPA given; size = total grades given.
- Axes: X = Courses taught, Y = Students taught.
- Use it to: Balance workloads and correlate load with performance; plan scheduling.

## 10) Content Type Analysis (`content_analysis`)
- What: Distribution of uploaded content types (pdf, pptx, mp4, etc.).
- Reading: Donut slice = share of a content type.
- Use it to: Ensure materials are accessible; standardize preferred formats.

---
Tips:
- PNGs are suitable for slides and portals; PDFs for print and archival.
- Pair these views with ML dashboards (see `../ml_reports/`) for deeper diagnostics.
