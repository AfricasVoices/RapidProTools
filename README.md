# RapidProTools
Contains scripts for fetching runs from Rapid Pro, and for inserting messages into a Rapid Pro development server.

## Installation Instructions

### Install RapidPro Locally
Follow the instructions on RapidPro's 
[developer web page](https://rapidpro.github.io/rapidpro/docs/development/).

The commit of RapidPro these instructions were tested against is `f988b82ed962c6bc3532393fa4f68cf301df6ebc`.

### Configure a RapidPro User
1. After starting the server, as described by the developer instructions referenced in the previous step, 
open [localhost:8000](http://localhost:8000) in a web browser. 

1. Enter an email address and click “Create Account”.

1. Follow the on-screen instructions to configure your user account and organisation.

### Create a Flow
There are two options here - build your own flow from scratch or use the one provided with this repository.

**To build your own flow:**
In RapidPro, select `Flows -> Create Flow`. This opens the visual flow editor.
Building a flow is fairly straightforward, but documentation for the editor is 
available [here](http://docs.rapidpro.io/#topic_6) if needed.

**To use the sample flow provided:**
In RapidPro, go to your organisation page (click the button in the top-right). 
Then choose `Settings Gear -> Import` and select `camelid-flow.json` from this repository. Finally, click `Import`.
This will upload the flow *and enable the 'camelid' trigger*. This trigger will start the flow whenever a user transmits 
the word "camelid" to one of your channels. Including this trigger is non-optional. If you wish to remove it, go to
`Triggers` then tick the checkbox next to the camelid trigger. Finally, click the Archive button which appeared.

To export a Flow for sharing, `Flows -> flow-to-be-shared -> settings gear -> Export -> Export`.

### Interacting with the Flow
The Flow editor includes an in-built simulator for testing (accessible by clicking the mobile phone icon).

Interactions with a flow via the simulator are not persisted to the RapidPro database.
For a run to persist, it must be run on a "channel". To add a channel:

1. Go to your organisation page
1. Choose the `Settings Gear -> Add Channel`
1. Select the type of channel you'd like to configure, and follow the onscreen instructions.
   For further support refer to the [RapidPro Documentation](http://docs.rapidpro.io/#topic_11).
   For interacting via the command line, a simple tool is provided with this repository. 
   See [Command Line Channel](#command-line-channel) for instructions on how to set up this channel.
1. Send your trigger message to the flow. If this is the provided sample, send the message "camelid".

### Command Line Channel
This repository includes a simple channel which can be used via the command line.
To set-up this channel:

1. Go to your organisation page.
1. Select `Settings Gear -> Add Channel -> External API`
1. Enter:
    1. `URN Type`: `Phone number`
    1. `Number`: Any phone number of your choosing. 
       Messages sent to the API will look like they have come from this number.
    1. `Country`: Can be any
    1. `Method`: `HTTP POST`
    1. `Content Type`: `URL-Encoded`
    1. `Max Length`: These instructions only tested with messages < max length
    1. `Send URL`: The endpoint at which the CLI channel will be available. If running locally, this will be:
       `http://localhost:8082/messages?from={{from}}&text={{text}}&to={{to}}&to_no_plus={{to_no_plus}}`.
       This URL cannot be changed. You will need to delete the channel and create a new one to correct mistakes.
    1. `Request Body`: Leave unchanged.
1. Click `Submit`. RapidPro now takes you to an External API Configuration page.
1. Examine the RapidPro URLs on the page to determine the base URL to which messages should be sent.
   If RapidPro is running locally, the end result will look like `http://localhost:8000/c/ex/<UUID>`.
1. Open `cli_channel.py`, included in this repository, in an editor.
1. Set the `SERVER_ADDR` constant to the RapidPro base URL you determined earlier. 
   You may optionally change the `PHONE_NUMBER` of the client you're simulating.
1. Update dependencies: `$ pipenv sync`.
1. Run the local channel: `$ pipenv run python cli_channel.py`.

You may now type a message to send to the channel.
If you send a flow trigger, the response from RapidPro will be printed to the console, and you will be given the
opportunity to send another message in response. If using the provided sample flow, start by sending "camelid".

### Polling for new Flow runs
1. Find your API token. This is available on your Organisation page of RapidPro.
1. Update dependencies: `$ pipenv sync`
1. Run: `$ pipenv run python poll_flow_runs.py <API-Token>`

The tool will output summary data on all runs, then poll for all runs again every few seconds.
Change to incremental fetching by uncommenting the last line.

### Downloading Runs to a TracedData data file
To export TracedData, use `$ pipenv run python fetch_runs.py`.

This script does not poll - it performs a single download and write to disk. 

### Load Testing
To automatically run the camelids flow many times, use the provided `camelid_runner.py` script.
The structure of this script is very similar to that of the command line channel, so follow
the [instructions for the command line channel](#command-line-channel), adapting to assign variables in, then to run
the script, `camelid_runner.py`. There is no need to create a new channel, provided you terminate `cli_channel.py`
before running `camelid_runner.py` 

### Adding Credits
Flow messages consume credits. By default, RapidPro only issues you with 1,000 "free" credits.
You can view your current credit balance on your organisation page.

To top up your credit balance on the dev server, insert a top-up entry into the orgs_topup table. 
Achieve this with the following command sequence:

1. `$ psql --user temba postgres`
1. `=# \c temba`
1. `=# SELECT * FROM orgs_org;`
1. Find your organisation in the resulting table, noting its `id` and the `created_by_id`.
1. `=# INSERT INTO orgs_topup (org_id, is_active, created_on, modified_on, price, credits, expires_on, 
    created_by_id, modified_by_id) VALUES (<org_id>, True, current_timestamp, current_timestamp,  0, 1000000, 
    '2050-01-01', <user_id>, <user_id>);`. 
    Set `<org_id>` to the `id` you noted in the previous step.
    Set `<user_id>` to the `created_by_id` you noted in the previous step.
   
Check if your new credits have been correctly added to your organisation by going to your organisation page and 
viewing your credit balance. 
This does not always update immediately - if this is still showing the old balance then click on this old balance
to view your Top Ups history, and look for the new entry here. The preview credit balance on the organisation page
will update to reflect the new balance once the current preview balance has been consumed.

### Exporting Flow Run Data via RapidPro
RapidPro can export all runs of a particular flow as a .xlsx file. To do this:

1. Go to `Flows`
1. Tick the checkbox of the flow you would like to export
1. Click the `Download Results` button (Excel logo), then `Ok`.

### Simulate Sending Individual Messages into RapidPro
If you don't care about receiving messages or triggering flows, RapidPro has a command line tool for easily
inputting messages into the system.

To use it, run this command from the RapidPro server directory:
`$ python manage.py msg_console --org <your-organisation-name>`.
The tool prints instructions for sending texts/changing the client phone number when it starts.
