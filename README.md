# RapidPro Flow Tester

## Installation Instructions

### Install RapidPro Locally
Follow the instructions on RapidPro's developer
[web page](https://rapidpro.github.io/rapidpro/docs/development/).

Note:
- A temba.settings file must be specified before the server can be run - the easiest way to create one is to
reuse the provided development configuration (`$ cp temba/settings.py.dev temba/settings.py`).
- The commit of RapidPro these instructions were tested against is `f988b82ed962c6bc3532393fa4f68cf301df6ebc`.

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
This will upload the flow *and the 'camelid' trigger*. This trigger will start the flow whenever a user transmits the 
word "camelid" to one of your channels. Including this trigger is non-optional. If you wish to remove it, go to
`Triggers` then tick the checkbox next to the camelid trigger. Finally, click the Archive button which appeared.

To export a Flow for sharing, `Flows -> flow-to-be-shared -> settings gear -> Export -> Export [sic.]`.

### Interacting with the Flow
The Flow editor includes an in-built simulator for testing (activate by clicking the mobile phone icon).

Interactions with a flow via the simulator are not persisted to the RapidPro database.
For a flow to persist, it must be run on a "channel". To add a channel:

1. Go to your organisation page
1. Choose the `Settings Gear -> Add Channel`
1. Select the type of channel you'd like to configure, and follow the onscreen instructions.
   For further support refer to the [RapidPro Documentation](http://docs.rapidpro.io/#topic_11).
   For interacting via the command line, a very simple tool is provided. Refer to the next section for instructions.
1. Send your trigger message to the flow. If this is the provided sample, send the message "camelid".

### Command Line Channel
This repository includes a simple channel which can be used via the command line.
To set-up this channel:

1. Go to your organisation page.
1. Select ``

### Polling for new Flow runs
TODO

### Load Testing
TODO s

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

### Simulate Sending Messages to RapidPro
RapidPro comes with a command line tool for this. From the rapidpro directory you cloned in step 1, run 
`$ python manage.py msg_console --org <organisation-set-in-step-2>`
The tool prints instructions for sending texts/changing the client phone number when it starts.

### Poll the Local Server
1. Set your API token in line 10 (<your-API-token>) of `poller.py`.
You can retrieve your API token from the “Your Account” page of the RapidPro web interface.

1. Install dependencies: `$ pip install -r pip-freeze.txt`

1. Run the polling demonstrator: `$ python poller.py`.
This will output the sender and text of all messages received by the system so far, and poll for new messages every 
2 seconds. New messages will be printed to the console as they arrive.


