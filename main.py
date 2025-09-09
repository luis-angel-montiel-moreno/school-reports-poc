import subprocess
import os
from glob import glob

# Step 1: Run storage.py to populate Delta tables
print("\n=== Running storage.py to populate data ===")
subprocess.run([
    "python", "storage.py"
], check=True)

# Step 2: Run analytics.py to generate analytics parquet files
print("\n=== Running analytics.py to generate analytics ===")
subprocess.run([
    "python", "analytics.py"
], check=True)

# Step 3: Run dashboards.py to generate dashboard PNGs
print("\n=== Running dashboards.py to generate dashboard PNGs ===")
subprocess.run([
    "python", "dashboards.py"
], check=True)

# Step 4: Display dashboard information
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
from glob import glob

print("\n=== Dashboards Generated ===")
reports_dir = "reports"
if os.path.exists(reports_dir):
    pngs = sorted(glob(f"{reports_dir}/*.png"))
    pdfs = sorted(glob(f"{reports_dir}/*.pdf"))
    
    print(f"Generated {len(pngs)} dashboard images:")
    for png in pngs:
        filename = os.path.basename(png)
        print(f"  - {filename}")
    
    print(f"\nGenerated {len(pdfs)} dashboard PDFs:")
    for pdf in pdfs:
        filename = os.path.basename(pdf)
        print(f"  - {filename}")
    
    print(f"\nAll files saved in: {os.path.abspath(reports_dir)}")
    
    # Display first few dashboards
    print("\n=== Displaying Sample Dashboards ===")
    for i, png in enumerate(pngs[:3]):  # Show first 3 dashboards
        print(f"Displaying: {os.path.basename(png)}")
        img = mpimg.imread(png)
        plt.figure(figsize=(12, 8))
        plt.imshow(img)
        plt.axis('off')
        plt.title(os.path.basename(png), fontsize=14, fontweight='bold')
        plt.show()
else:
    print("No reports directory found.")

# Optional: Run ML pipeline if requested via env var RUN_ML=1
run_ml = os.environ.get("RUN_ML", "0") == "1"
if run_ml:
    print("\n=== Running ML analytics (ml_analytics.py) ===")
    subprocess.run(["python", "ml_analytics.py"], check=True)

    print("\n=== Running ML dashboards (ml_dashboards.py) ===")
    subprocess.run(["python", "ml_dashboards.py"], check=True)

    print("\n=== ML Reports Generated ===")
    ml_reports_dir = "ml_reports"
    if os.path.exists(ml_reports_dir):
        ml_pngs = sorted(glob(f"{ml_reports_dir}/*.png"))
        ml_pdfs = sorted(glob(f"{ml_reports_dir}/*.pdf"))
        print(f"Generated {len(ml_pngs)} ML dashboard images:")
        for f in ml_pngs:
            print(f"  - {os.path.basename(f)}")
        print(f"\nGenerated {len(ml_pdfs)} ML dashboard PDFs:")
        for f in ml_pdfs:
            print(f"  - {os.path.basename(f)}")
        print(f"\nAll ML files saved in: {os.path.abspath(ml_reports_dir)}")
    else:
        print("No ml_reports directory found.")
