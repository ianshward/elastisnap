var aws = require("aws-lib");
var _ = require("underscore");
var argv = require('optimist').argv;
var fs = require('fs');
var exec = require('child_process').exec;
var Step = require('step');

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
// Allow options command-line overrides
_.each(argv, function(v, k) {
    options[k] = argv[k] || options[k];
});

if (!options.awskey || !options.awssecret) {
    console.log("Must provide all of awskey, awssecret --config parameters")
    process.exit(1);
}

// Allow a single job to be passed in as cli args
if (argv.instanceid) {
    var job = {};
    _(['instanceid', 'pool', 'devices', 'description']).each(function(key) {
        if (!_(_(argv).keys()).include(key)) throw new Error('Option ' + key + ' is required when running a single job.');
        job[key] = argv[key];
    });
    options.jobs = [job];
}

// version 2010-08-31 supports the 'Filter' parameter.
ec2 = aws.createEC2Client(options.awskey, options.awssecret,
  {version: '2010-08-31', host: 'ec2.' + options.region + '.amazonaws.com'}
);


// This does the following:
//   - Gets the volume-id based on device + instance-id
//   - Creates a snapshot based on that volume-id
//   - Deletes the oldest snapshot of the pool size is exceeded
function run(selfInstanceId) {
    _.each(options.jobs, function(job) {
        var id = job.instanceid == 'self' ? selfInstanceId : job.instanceid;
        var devices = job.devices.split(/\s*,\s*/);
        _.each(devices, function(device) {
            var params = {};
            params['Filter.1.Name'] = 'attachment.device';
            params['Filter.1.Value.1'] = device;
            params['Filter.2.Name'] = 'attachment.instance-id';
            params['Filter.2.Value.1'] = id;
            ec2.call('DescribeVolumes', params, function(err, result) {
                if (err) throw err;
                if (!result.volumeSet.item) throw new Error('Volume not found on instance ' + id + ' at ' + device);
                var volume = result.volumeSet.item.volumeId;
                var description = job.description + ' ' + device + ' ' + id;
                ec2.call('CreateSnapshot', {VolumeId: volume, Description: description}, function(err, result) {
                    if (err) throw err;
                    if (!result.Errors) {
                        var params = {};
                        params['Owner'] = 'self';
                        params['Filter.1.Name'] = 'description';
                        params['Filter.1.Value.1'] = description;
                        ec2.call("DescribeSnapshots", params, function(err, result) {
                            if (err) throw err;
                            if (!result.Errors) {
                                if (result.snapshotSet.item) {
                                    var snapshots = result.snapshotSet.item;
                                    if (snapshots.length > job.pool) {
                                        snapshots = _.sortBy(snapshots, function(snapshot) { return new Date(snapshot.startTime).getTime(); });
                                        // Delete oldest snapshot within the pool. Assumes pool is not already exceeded.
                                        ec2.call('DeleteSnapshot', {SnapshotId: snapshots[0].snapshotId}, function(err, result) {
                                            if (err) throw err;
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
    exec('wget -q -O - http://169.254.169.254/latest/meta-data/instance-id',
        function (error, stdout, stderr) {
            cb(stdout);
    });
}

getInstanceId(function(instanceId) {
    run(instanceId);
});
