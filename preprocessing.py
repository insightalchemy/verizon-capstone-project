import pandas as pd
import json

df = pd.read_csv("webscale-core-logs-5min.csv")
print(df.columns.to_list())
print(df['log.level'].unique())
print(df['kubernetes.node.name'].unique())

class Processor:
  def __init__(self, file):
    self.file = file
    try:
      self.df = pd.read_csv(file)
    except Exception as e:
      print("Error reading file: {}".format(e))

  def convert_to_json(self):
    json_data = self.df.to_json(orient = "records")
    return json_data

  def delete_columns(self, columns_to_delete):
    self.df.drop(columns = columns_to_delete, inplace = True, errors = "ignore")

  def save_to_csv(self, output_file):
    self.df.to_csv(output_file, index = False)

  def node_logs(self):
    group = self.df.groupby(["kubernetes.node.name", "log.level"]).size().reset_index(name = "count")
    result = {}
    for _, row in group.iterrows():
      node = row["kubernetes.node.name"]
      log_type = row["log.level"]
      count = row["count"]

      if node not in result:
        result[node] = {}
      result[node][log_type] = count
    return result

p = Processor("webscale-core-logs-5min.csv")
results = p.node_logs()
print(json.dumps(results, indent = 2))
