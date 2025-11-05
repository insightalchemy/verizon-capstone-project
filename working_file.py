import pandas as pd
import json
df = pd.read_csv("webscale-core-logs-5min.csv")
print(df.columns.to_list())
print(df["@timestamp"].head())
print(df['log.level'].unique())
print(df['kubernetes.node.name'].unique())

class Processor:
  # Initializing the clas with the file
  def __init__(self, file):
    self.file = file
    try:
      self.df = pd.read_csv(file)
    except Exception as e:
      print("Error reading file: {}".format(e))

  # Deleting columns in df based on input
  def delete_columns(self, columns_to_delete):
    self.df.drop(columns = columns_to_delete, inplace = True, errors = "ignore")

  # 1.1.1.2
  def node_pod_grouping(self):
    new_df = self.df.dropna(subset = ["kubernetes.node.name", "kubernetes.pod.name"])
    new_df = new_df[(new_df["kubernetes.node.name"] != "-") & (new_df["kubernetes.pod.name"] != "-")]
    node_pod_dict = (new_df.groupby("kubernetes.node.name")["kubernetes.pod.name"].unique().apply(list).to_dict())
    print(json.dumps(node_pod_dict, indent = 2))

  # 1.1.1.3
  def log_level_counts(self):
    new_df = self.df.dropna(subset = ["kubernetes.node.name", "log.level"])
    new_df = new_df[(new_df["kubernetes.node.name"] != "-") & (new_df["log.level"] != "-")]
    total_counts = (new_df["log.level"].value_counts().to_dict())
    node_level_counts = (new_df.groupby(["kubernetes.node.name", "log.level"]).size().unstack(fill_value = 0).to_dict(orient = "index"))
    result = {"Log Levels (Total)": total_counts, "Log Levels (Per Node)": node_level_counts}
    print(json.dumps(result, indent = 2))

  # 1.1.1.4
  def time_between_logs(self):
    self.df["@timestamp"] = pd.to_datetime(self.df["@timestamp"], format="%b %d, %Y @ %H:%M:%S.%f", errors="coerce")
    new_df = self.df.dropna(subset=["kubernetes.node.name", "@timestamp"])
    new_df = new_df[new_df["kubernetes.node.name"] != "-"]
    new_df = new_df.sort_values(["kubernetes.node.name", "@timestamp"])
    node_time_diffs = {}
    for node, group in new_df.groupby("kubernetes.node.name"):
        timestamps = list(group["@timestamp"])
        time_diffs = [(timestamps[i] - timestamps[i - 1]).total_seconds() for i in range(1, len(timestamps))]
        node_time_diffs[node] = time_diffs
    return node_time_diffs

  def convert_to_json(self):
    json_data = self.df.to_json(orient = "records")
    return json_data

  def save_to_csv(self, output_file):
    self.df.to_csv(output_file, index = False)

# Executing task 1.1.1.2
p = Processor("webscale-core-logs-5min.csv")
node_pod_grouping = p.node_pod_grouping()
print(json.dumps(node_pod_grouping, indent = 2))

# Executing task 1.1.1.3
log_level_counts = p.log_level_counts()
print(json.dumps(log_level_counts, indent = 2))

# Executing task 1.1.1.4
time_between_logs = p.time_between_logs()
print(json.dumps(time_between_logs, indent = 2))
