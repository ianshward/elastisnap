var aws = require("aws-lib"),
    _ = require("underscore"),
    argv = require('optimist').argv,
    fs = require('fs'),
    sh = require('sh');

if (!argv.config) {
    console.log("Must provide --config argument which points to json settings file, such as --config settings.json");
    process.exit(1);
}

var options = {};
try {
    var config = JSON.parse(fs.readFileSync(argv.config, 'utf8'));
    for (var key in config) {
        options[key] = config[key];
    }
} catch(e) {
   console.warn('Invalid JSON config file: ' + options.config);
   throw e;
}

if (!options.awskey ||
    !options.awssecret) {
    console.log("Must provide all of awskey, awssecret, pool, description, and volume as --config parameters")
    process.exit(1);
}

// version 2010-08-31 supports the 'Filter' parameter.
ec2 = aws.createEC2Client(options.awskey, options.awssecret, {version: '2010-08-31'});

var jobs = options.jobs;

// This does the following:
//   - Gets the volume-id based on device + instance-id
//   - Creates a snapshot based on that volume-id
//   - Deletes the oldest snapshot of the pool size is exceeded
function run(selfInstanceId) {
    _.each(jobs, function(job, key) {
        var id = key == 'self' ? selfInstanceId : key;
        var devices = job.devices.split(/\s*,\s*/);
        _.each(devices, function(device) {
            var params = {};
            params['Filter.1.Name'] = 'attachment.device';
            params['Filter.1.Value.1'] = device;
            params['Filter.2.Name'] = 'attachment.instance-id';
            params['Filter.2.Value.1'] = key;
            ec2.call('DescribeVolumes', params, function(result) {
                var volume = result.volumeSet.item.volumeId;
                var description = job.description + ' ' + device + ' ' + id;
                ec2.call('CreateSnapshot', {VolumeId: volume, Description: description}, function(result) {
                    if (!result.Errors) {
                        var params = {};
                        params['Owner'] = 'self';
                        params['Filter.1.Name'] = 'description';
                        params['Filter.1.Value.1'] = description;
                        ec2.call("DescribeSnapshots", params, function(result) {
                            if (!result.Errors) {
                                if (result.snapshotSet.item) {
                                    var snapshots = result.snapshotSet.item;
                                    if (snapshots.length > job.pool) {
                                        snapshots = _.sortBy(snapshots, function(snapshot) { return new Date(snapshot.startTime).getTime(); });
                                        // Delete oldest snapshot within the pool. Assumes pool is not already exceeded.
                                        ec2.call('DeleteSnapshot', {SnapshotId: snapshots[0].snapshotId}, function(result) {
                                            // Do nothing
                                        });
                                    }
                                }
                            }
                        });
                    }
                });
            });
        });
    });
}

function getInstanceId(cb) {
    sh('wget -q -O - http://169.254.169.254/latest/meta-data/instance-id').result(function(id) {
        cb(id);
    });
}

getInstanceId(function(instanceId) {
    run(instanceId);
});
