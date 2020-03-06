# MNO Analysis Tools
Contains scripts for fetching raw messages, processing the raw messages and a web page that visualizes the processed raw messages through graphs for MNO analysis. The aim of the analysis is to aid in the design of the MNo downtime alert system. 

The scripts contain the implementation of the following declarations made in the `MNo downtime alert` design document:

(a) Compute max window of time with 0 messages.<br> 
(b) Compute `rN = d(n)/d(t)` i.e message difference between two firebase time periods (the time period for firebase is a constant number).<br>
(c) Plot log scale line graphs showing:
 - Messages received per time over the period of the project.
 - rN
 - a Bar plot indicating the periods with the maximum window of time with 0 messages.


## Usage
To generate the MNO analysis graphs follow the following steps, executed in sequence:\
(1) Fetch raw messages\
(2) Process the raw messages to produce the outputs required for analysis\
(3) Upload the `index` web page

### 1. Fetch Raw Messages
This stage fetches all the raw messages from Rapid Pro.
To use, run the following command from the `mno_analysis_tools` directory: 

```
$ python fetch_raw_messages.py <domain> <token> <raw_messages_file_path>
```

where:
- `domain` is the domain that the instance of Rapid Pro is running on
- `token` is the organisation access token for authenticating to the instance
- `raw_messages_file_path`  is a relative path to the directory in which the raw messages data should be downloaded to. Downloaded Raw Messages file is saved to `./raw_messages.json`.

### 2. Generate Outputs
This stage processes the raw data to produce outputs from the computations below as Json for MNO downtime analysis.
Ensure you use the same start and end date for each script to derive insight from the analysis.
To use, run the following commands from the `mno_analysis_tools` directory:

(a) Compute maximum window of time with 0 messages
```
$ python compute_window_of_downtime.py <raw_messages_file_path> <window_of_downtimes_output_file_path> <target_operator> <target_message_direction> <start_date> <end_date>
```

where:
- `raw_messages_file_path` is a relative path to the directory containing the file to read serialized Rapid Pro message data from
- `window_of_downtimes_output_file_path` is a relative path to the directory where the file to write the computed windows of downtime data downloaded as json. Downloaded data file is saved to either `./incoming_msg_downtime.json` or `outgoing_msg_downtime.json` depending on the target message direction. 
- `target_operator` Operator to analyze for downtime
- `target_message_direction` Direction of messages to limit the search for downtime to
- `start_date` The start date as ISO 8601 string from which the window of downtime will be computed
- `end_date` The end date as ISO 8601 string to which the window of downtime computation will end 

(b) Compute the number of messages in each interval between the given start and end dates
```
$ python compute_messages_per_period.py <raw_messages_input_file_path> <computed_messages_per_period_output_file_path> <target_operator> <target_message_direction> <start_date> <end_date> <time_frame>
```

where:
- `raw_messages_input_file_path` is a relative path to the file to read serialized Rapid Pro message data from
- `computed_messages_per_period_output_file_path` is a relative path to the file to write the computed messages per period data. Downloaded data file is saved to either `./incoming_msg.json` or `outgoing_msg.json` depending on the target message direction.
- `target_operator` Operator to analyze
- `target_message_direction` Direction of messages to limit the search for downtime to
- `start_date` The start date as ISO 8601 string from which the number of messages will be computed
- `end_date` The end date as ISO 8601 string to which the number of messages computation will end 
- `time_frame` The time frame (DD:HH:MM:SS) to generate dates in intervals between the start and end date

(c) Compute message difference between two firebase time periods (the time period for firebase is a constant number)
```
python compute_msg_difference_btwn_periods.py <raw_messages_input_file_path> <computed_messages_per_period_output_file_path> <target_operator> <target_message_direction> <start_date> <end_date> <time_frame>
```

where:
- `raw_messages_input_file_path` is a relative path to the directory containing the file to read serialized Rapid Pro message data from
- `message_difference_output_file_path` is a relative path to the file to write the messages difference between two periods data. Downloaded data file is saved to either `./incoming_msg_diff_per_period.json` or `outgoing_msg_diff_per_period.json` depending on the target message direction.
- `target_operator` Operator to analyze
- `target_message_direction` Direction of messages to limit the search for downtime to
- `start_date` The start date as ISO 8601 string from which the number of messages will be computed
- `end_date` The end date as ISO 8601 string to which the number of messages computation will end
- `time_frame` is an optional argument for the time frame (HH:MM:SS) to generate dates in intervals between the start and end date. The default time frame is 10 seconds.

### 3. Generate Graphs
This stage generates the MNO analysis graphs. 
To use, ensure the you have the data from the previous step then upload the index web page


