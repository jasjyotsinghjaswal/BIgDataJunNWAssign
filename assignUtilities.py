# Define a function for validating an Email
def mail_validator(email):
    import re
    # Make a regular expression for validating an Email
    regex = '^\w+([\.-]?\w+)*@\w+([\.-]?\w+)*(\.\w{2,3})+$'
    # pass the regualar expression and the string in search() method
    if (re.search(regex, email)):
        return "Valid Email"
    else:
        return "Invalid Email"


# Define a function for iterating list of keys in a dictionary and appending a_ to each key
def modify_dict_keys(row):
    import json
    dicrow = json.loads(row)
    newdicrow = {}
    for key in dicrow:
        newdicrow["a_" + key] = dicrow[key]
    return newdicrow


# Define a function for grouping records for each product key and recreating Product and Quarter sales data in same row
def group_item_sales(current_item, pandas_dataset, group_key_values):
    print("Current Item is " + current_item)
    item_dataset = pandas_dataset[pandas_dataset.Item == current_item]
    record_dict = {"Item": current_item}
    for quarter in group_key_values:
        record_dict[quarter] = int(item_dataset.loc[item_dataset.Quarter == quarter, ["Sales"]].iloc[0, 0])
    return record_dict


# Define a function to log to HDFS path
def logToHDFS(message, path):
    from hdfs import InsecureClient
    # Connecting to Webhdfs by providing hdfs host ip and webhdfs port (50070 by default)
    client_hdfs = InsecureClient('http://' + "quickstart.cloudera" + ':50070', user="cloudera")
    # Write to hdfs
    with client_hdfs.write(path, encoding='utf-8', append=True) as writer:
        writer.write(message + "\n")
        writer.flush()
