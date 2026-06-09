# 🚀 LakeForge Simple Pipeline Guide

**For Non-Coders & Beginners**

---

## 📖 What is This?

This is a **beginner-friendly data pipeline** that anyone can run without coding experience.

It demonstrates how to:
- ✅ Load customer data
- ✅ Check data quality
- ✅ Save data securely
- ✅ Validate data integrity
- ✅ Generate trust reports

---

## 🎯 How to Use

### Step 1: Open the Notebook
1. Navigate to: `/Users/jayarampogakula@gmail.com/lakeforge/notebooks/demo/`
2. Open: **`simple_pipeline_guide.py`**

### Step 2: Run the Pipeline
**Option A - Run Everything at Once:**
1. Click the "Run All" button at the top of the notebook
2. Wait 2-3 minutes for all cells to complete
3. Review the results!

**Option B - Run Step by Step:**
1. Click the "Run" button (▶️) on each cell
2. Wait for the cell to finish (you'll see a green checkmark ✅)
3. Move to the next cell
4. Repeat until all cells are complete

### Step 3: Review Results
Scroll to the bottom to see:
- 📊 Overall Trust Score
- ✅ Validation Results
- 📈 Data Quality Metrics

---

## 🧩 What Each Step Does

| Step | What It Does | Time |
|------|-------------|------|
| **Step 1** | Loads required tools and libraries | ~10 sec |
| **Step 2** | Creates 10 sample customer records | ~5 sec |
| **Step 3** | Saves data to Bronze layer with audit tracking | ~20 sec |
| **Step 4** | Runs 4 data quality checks (nulls, duplicates, formats) | ~15 sec |
| **Step 5** | Runs 3 trust validations (counts, explosions, spikes) | ~15 sec |
| **Step 6** | Generates comprehensive trust report | ~10 sec |
| **Summary** | Shows final results and next steps | Instant |

**Total Time: ~2-3 minutes**

---

## 📊 Understanding Your Results

### Trust Score Levels:

🟢 **95-100% (EXCELLENT)**
- Your data is production-ready
- High confidence, no issues found
- ✅ Safe to use

🟡 **85-94% (GOOD)**
- Minor issues detected
- Generally safe to proceed
- ⚠️ Review warnings

🟠 **70-84% (ACCEPTABLE)**
- Some quality concerns
- Proceed with caution
- ⚠️ Address issues soon

🟠 **50-69% (POOR)**
- Significant problems found
- Needs immediate attention
- ❌ Fix before using

🔴 **Below 50% (CRITICAL)**
- Critical data quality issues
- Do NOT use this data
- ❌ Must fix immediately

---

## 🆘 Troubleshooting

### Problem: Cell shows an error
**Solution:**
1. Read the error message carefully
2. Check if you ran all previous cells first
3. Try running the cell again
4. If still failing, restart: `Run All` from the beginning

### Problem: "Table not found" error
**Solution:**
- Make sure you ran Step 3 (Bronze Layer) successfully
- Check that the cell showed "✅ Data saved to Bronze layer!"

### Problem: Notebook is slow
**Solution:**
- This is normal for first run (Spark initialization)
- Subsequent runs will be faster
- Total time should be 2-3 minutes

### Problem: Import errors
**Solution:**
- Ensure LakeForge is installed at `/Workspace/Users/jayarampogakula@gmail.com/lakeforge/`
- Restart the notebook and run again

---

## 🎓 Next Steps

### For Beginners:
1. ✅ Run the notebook as-is to see how it works
2. ✅ Review the trust report results
3. ✅ Read the comments in each cell to understand what's happening

### For Your Own Data:
1. **Replace Step 2** with your actual data source:
   - CSV file from Unity Catalog Volume
   - Excel file
   - Database connection
   - API endpoint

2. **Customize Step 4** validation rules:
   - Add checks for your specific columns
   - Set appropriate thresholds
   - Define business rules

3. **Adjust Step 5** trust validations:
   - Configure row count tolerances
   - Set duplicate explosion ratios
   - Define null spike thresholds

---

## 📚 Additional Resources

- **Full Documentation**: `/lakeforge/README.md`
- **Advanced Testing**: `/lakeforge/docs/testing/TESTING_GUIDE.md`
- **Configuration Examples**: `/lakeforge/configs/`
- **Module Documentation**: `/lakeforge/PHASE1_SUMMARY.txt`

---

## 💡 Quick Tips

✅ **DO:**
- Run cells in order from top to bottom
- Wait for each cell to complete before moving to the next
- Review the output of each cell
- Read the summary at the end

❌ **DON'T:**
- Skip cells (they depend on each other)
- Edit code unless you know what you're doing
- Run cells out of order
- Ignore error messages

---

## 🔧 Clean Up

After you're done, you can clean up the demo data:

1. Add a new cell at the bottom
2. Paste this code:
```python
spark.sql("DROP TABLE IF EXISTS bronze.raw.customers_demo")
print("✅ Demo table deleted!")
```
3. Run the cell

---

## ✉️ Need Help?

- **Documentation Issues**: Check `/lakeforge/README.md`
- **Technical Support**: Contact your data engineering team
- **Bug Reports**: Document the error and steps to reproduce

---

**🎉 You're ready to go! Open `simple_pipeline_guide.py` and click "Run All"!**
