var aws = require('aws-lib');
var _ = require('underscore');
var argv = require('optimist').argv;
var fs = require('fs');
var exec = require('child_process').exec;
var Step = require('step');

if (!argv.config) {
    console.log('Must provide --config argument which points to json settings file, such as --config settings.json');
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
    console.log('Must provide all of awskey, awssecret --config parameters')
    process.exit(1);
}

// Allow a single job to be passed in as cli args
if (argv.instanceid) {
    var job = {};
    _(['instanceid', 'pool', 'device', 'description']).each(function(key) {
        if (!_(_(argv).keys()).include(key)) throw new Error('Option ' + key + ' is required when running a single job.');
        job[key] = argv[key];
    });
    options.jobs = [job];
}

// version 2010-08-31 supports the 'Filter' parameter.
ec2 = aws.createEC2Client(options.awskey, options.awssecret,
  {version: '2010-08-31', host: 'ec2.' + options.region + '.amazonaws.com'}
);

Step(function() {
    exec('wget -q -O - http://169.254.169.254/latest/meta-data/instance-id', this);
}, function(err, selfInstanceId) {
    if (err) throw err;
    var group = this.group();
    _.each(options.jobs, function(job) {
        job.instanceid = job.instanceid === 'self' ? selfInstanceId : job.instanceid;
        job.description = job.description + ' ' + job.device + ' ' + job.instanceid;
        var params = {};
        params['Filter.1.Name'] = 'attachment.device';
        params['Filter.1.Value.1'] = job.device;
        params['Filter.2.Name'] = 'attachment.instance-id';
        params['Filter.2.Value.1'] = job.instanceid;
        ec2.call('DescribeVolumes', params, group());
    });
}, function(err, volumes) {
    if (err) throw err;
    var group = this.group();
    _(volumes).each(function(v, k) { 
        var job = options.jobs[k];
        ec2.call('CreateSnapshot', {VolumeId: v.volumeSet.item.volumeId, Description: job.description}, group()); 
    });
}, function(err) {
    if (err) throw err;
    var params = {};
    params['Owner'] = 'self';
    ec2.call('DescribeSnapshots', params, this);
}, function(err, result) {
    if (err) throw err;
    _(options.jobs).each(function(j) { 
        snapshots = _(result.snapshotSet.item).chain()
            .filter(function(snapshot) {
                return snapshot.description === j.description;
            })
            .sortBy(function(snapshot) { return -(new Date(snapshot.startTime).getTime()); })
            .rest(j.pool)
            .value();
        _(snapshots).each(function(snapshot) {
            ec2.call('DeleteSnapshot', {SnapshotId: snapshot.snapshotId}, function(err, result) {
                if (err) throw err;
            });
        });
    });
});
