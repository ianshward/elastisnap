Take rotating Amazon AWS EBS snapshots with this small node.js script.

* Define what you want to snapshot based on instance-id + devices on that instance.
* Put the script on an EC2 and use 'self' as one of the "jobs" keys to refer to that instance itself.
* Handles multiple devices at a time on various instances.

See setting.json for sample definitions. Options include:

pool - the number of snapshots to maintain before destroying old ones.
device - name of device that should be snapshotted.
description - used as the description for the snapshot along with the device name and the instance ID.

`node index.js --config=settings.json` ... throw that in a hourly, daily, etc. cronjob and set the 'pool' size and you've got rotating snapshots.
