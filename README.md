# GreenLake-Scripts
API Scripts for making changes to GreenLake


## greenlake_swap_subscriptions.py
  This will all you to change the subscription on a device with another subscription in your account.

Opitons:
* -h, --help            show this help message and exit
* --serials SERIALS     Path to .txt file – one serial number per line.
* --new-subscription-key NEW_SUB_KEY
	* Subscription key to apply (e.g. SUB-XXXX-XXXX).
* --client-id CLIENT_ID
	* OAuth2 Client ID.
* --client-secret CLIENT_SECRET
	* OAuth2 Client Secret.
* --dry-run             Simulate all changes without calling mutating APIs.
* --delay DELAY         Seconds between devices (default: 1).
  
