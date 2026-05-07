import awswrangler as wr
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec

GOLD_BUCKET = "s3://dataforge-gold-dev-eu-central-1"

print("Reading Gold layer from S3...")
df_loc       = wr.s3.read_csv(f"{GOLD_BUCKET}/top_locations.csv")
df_source    = wr.s3.read_csv(f"{GOLD_BUCKET}/jobs_by_source.csv")
df_remote    = wr.s3.read_csv(f"{GOLD_BUCKET}/remote_vs_onsite.csv")
df_trend     = wr.s3.read_csv(f"{GOLD_BUCKET}/jobs_trend.csv")
df_companies = wr.s3.read_csv(f"{GOLD_BUCKET}/top_companies.csv")

fig = plt.figure(figsize=(18, 14))
fig.suptitle("German Data Job Market Dashboard", fontsize=18, fontweight="bold", y=0.98)
gs = gridspec.GridSpec(2, 3, figure=fig, hspace=0.45, wspace=0.35)

# 1. Top locations — horizontal bar
ax1 = fig.add_subplot(gs[0, :2])
top10 = df_loc.head(10).sort_values("job_count")
ax1.barh(top10["location"], top10["job_count"], color="#3498db")
ax1.set_title("Top 10 Cities for Data Jobs")
ax1.set_xlabel("Job Count")
for i, v in enumerate(top10["job_count"]):
    ax1.text(v + 1, i, str(v), va="center", fontsize=9)

# 2. Jobs by source — pie
ax2 = fig.add_subplot(gs[0, 2])
ax2.pie(df_source["job_count"], labels=df_source["source"],
        autopct="%1.1f%%", colors=["#2ecc71", "#e74c3c"], startangle=90)
ax2.set_title("Jobs by Source")

# 3. Remote vs onsite — pie
ax3 = fig.add_subplot(gs[1, 0])
ax3.pie(df_remote["job_count"], labels=df_remote["work_type"],
        autopct="%1.1f%%", colors=["#9b59b6", "#f39c12"], startangle=90)
ax3.set_title("Remote vs On-site\n(Arbeitnow)")

# 4. Jobs trend — line
ax4 = fig.add_subplot(gs[1, 1])
ax4.plot(df_trend["date"], df_trend["new_jobs"], marker="o", color="#e74c3c", linewidth=2)
ax4.set_title("Jobs Added Over Time")
ax4.set_xlabel("Date")
ax4.set_ylabel("New Jobs")
ax4.tick_params(axis="x", rotation=30)

# 5. Top companies — horizontal bar
ax5 = fig.add_subplot(gs[1, 2])
top8 = df_companies.head(8).sort_values("job_count")
ax5.barh(top8["company"], top8["job_count"], color="#1abc9c")
ax5.set_title("Top 8 Hiring Companies")
ax5.set_xlabel("Job Count")
for i, v in enumerate(top8["job_count"]):
    ax5.text(v + 0.3, i, str(v), va="center", fontsize=8)

output = "analytics/job_market_dashboard.png"
plt.savefig(output, dpi=150, bbox_inches="tight")
print(f"Dashboard saved to {output}")
