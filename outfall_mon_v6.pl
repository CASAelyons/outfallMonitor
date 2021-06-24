##################################################################################################
#####WRITTEN BY ERIC LYONS 1/2020 for CASA, UNIVERSITY OF MASSACHUSETTS##########################
##################################################################################################
#  TESTED FUNCTIONALITY:                                                                         #
#  OUTFALL_MON.PL
#  -RECURSIVELY MONITORS DIRECTORIES FOR INCOMING OUTFALL FILES
#  -MOVING WINDOW    
#  -COLLECT QPE 
#                                                                                                  #
##################################################################################################

use POSIX qw(setsid);
use AutoLoader qw/AUTOLOAD/;
use lib "/home/elyons/perl/";
use File::Monitor;
use threads;
use threads::shared;
use DateTime;
use GD::Graph;
use GD::Graph::lines;
use DBI;
use JSON;
use DateTime;
use DateTime qw(from_epoch);
use List::Util qw( min max );
use Scalar::Util qw(looks_like_number);
use Digest::SHA qw(hmac_sha256_hex);
use Encode qw(encode);
use REST::Client;
use WWW::Curl::Easy;
use MIME::Lite;
use Geo::JSON;

our $input_data_dir;
our $outfall_id;
our $field_to_monitor;
our $field_to_read;
our $data_format;
&command_line_parse;

#&daemonize;

our @delta_ts;
our $latest_flow;
our @timestamps;
our $accumulating = 0;
our $waiting = 0;
our $acc_start;
our $initial_delta = 0;
our @flowarr;
our @timearr;
our @permitvals;
our @permittimes;

our $event_no = 0;
our $gauge_online; #the permitted rain gauge
our @totarr;
our @gaugetotarr;
our @accumtimearr;
push @totarr, 0;
push @gaugetotarr, 0;
our $threshold = .1;
our $initialized = 0;
our @initialization;
our $baseflow;
our $basedeviation;
our $maybe = 0;  #to account for oneoff type noise

#our $startEpoch;
our $lat;# = 32.9199;
our $lon;# = -97.0335;
our $loc = "Outfall_" . $outfall_id; #059";
our $outfall_geojson_fn = "/home/elyons/perl/dfwairport_outfalls.geojson";
our $possible_endtime;
our $waitingPeriodGaugeTotal = 0;
our $runningGaugeTotal = 0;
our $runningTotal = 0;
our $db_host = 'localhost';
our $db_user = 'dfw';
our $db_pass = 'outfall';
our $db_name = 'dfwairport';
our $db = "dbi:Pg:dbname=${db_name};host=${db_host}";
my $dbh = DBI->connect($db, $db_user, $db_pass, { RaiseError => 1 })
    or die $DBI::errstr;
print "connected to db!\n";

#update database with field being monitored
print $field_to_monitor . " " . $outfall_id . "\n";
my $sth = $dbh->prepare("UPDATE outfalls SET ProductType='${field_to_monitor}' WHERE Id = '${outfall_id}';");
my $rv = $sth->execute
    or warn $sth->errstr;
$sth->finish;

#set abovebaseflow and eventid to null because we don't know if we're above or in an event upon startup 
$sth = $dbh->prepare("UPDATE outfalls SET AboveBaseFlow=null WHERE Id = '${outfall_id}';");
$rv = $sth->execute
    or warn $sth->errstr;
$sth->finish;

$sth = $dbh->prepare("SELECT EventId from outfalls WHERE Id = '${outfall_id}';");
$rv = $sth->execute
    or warn $sth->errstr;
my $eventid = $sth->fetchrow();
$sth->finish;

if ( defined $eventid) {
    my $unknownstr = "outcome unknown: monitoring code restarted";
    $sth = $dbh->prepare("UPDATE events SET outcome = ? WHERE eventId = '${eventid}';");
    $sth->bind_param(1, $unknownstr);
    $rv = $sth->execute
	or warn $sth->errstr;
    $sth->finish;
}

$sth = $dbh->prepare("UPDATE outfalls SET EventId=null WHERE Id = '${outfall_id}';");
$rv = $sth->execute
    or warn $sth->errstr;
$sth->finish;

#set waiting72hrs and accumulatingrainfall to false... we're starting from the beginning here
$sth = $dbh->prepare("UPDATE outfalls SET Waiting72hrs='false' WHERE Id = '${outfall_id}';");
$rv = $sth->execute
    or warn $sth->errstr;
$sth->finish;

$sth = $dbh->prepare("UPDATE outfalls SET AccumulatingRainfall=null WHERE Id = '${outfall_id}';");
$rv = $sth->execute
    or warn $sth->errstr;
$sth->finish;

$dbh->disconnect;

our $to = 'elyons19@hotmail.com, elyons@engin.umass.edu';
our $from = 'noreply@casaalerts.com';
our $subject = 'CASA automated notification for ' . $loc;

get_outfall_ll($outfall_geojson_fn);

my $file_mon = new threads \&file_monitor;

sleep 900000000;

sub file_monitor {

    my $dir_monitor = File::Monitor->new();

    $dir_monitor->watch( {
	name        => "$input_data_dir",
	recurse     => 1,
	callback    => \&new_files,
			 } );

    $dir_monitor->scan;

    for ($i=0; $i < 9000000000; $i++) {
	my @changes = $dir_monitor->scan;
	system("date -u");
	sleep 20;
    }

    sub new_files
    {
	my ($name, $event, $change) = @_;
	my @tmp = ();

	@new_files = $change->files_created;
	my @dels = $change->files_deleted;
	print "Added: ".join("\nAdded: ", @new_files)."\n" if @new_files;
	foreach $file (@new_files) {
	    sleep 1;
	    my $pathstr;
	    my $filename;
	    ($pathstr, $filename) = $file =~ m|^(.*[/\\])([^/\\]+?)$|;
	    print "filename: " . $filename . "\n";
	    #my $suffix = substr($file, -3, 3);
	    my $prefix = substr($filename, 0, 7);
	    if ($prefix ne "Outfall"){
		next;
	    }

	    print "opening Outfall \n";
	    open(my $data, '<', $file) or die "Could not open '$file' $!\n";
	    if ($data_format == 1) {
		print "From ftp...\n";
		print "data 0: " . <$data> . "\n";
		print "data 1: " . <$data> . "\n";
		print "data 2: " . <$data> . "\n";
		print "data 3: " . <$data> . "\n";
	    }
	    while (my $line = <$data>) {
		print "line: " . $line . "\n";
		my @fields = split "," , $line;
		    		    
		my $timefield;
		if ($data_format == 1) {
		    $timefield = timestringFormatSwap($fields[0]);
		}
		else {
		    $timefield = $fields[0];
		}
		my $tstamptz = timestringToTimestampTZ($timefield);
		
		my $dbh = DBI->connect($db, $db_user, $db_pass, { RaiseError => 1 })
		    or die $DBI::errstr;
		
		my $sth = $dbh->prepare("UPDATE outfalls SET lastData='${tstamptz}' WHERE Id = '${outfall_id}';");
		my $rv = $sth->execute
		    or warn $sth->errstr;
		$sth->finish;
		$dbh->disconnect;

		#error check
		if ((!looks_like_number($fields[$field_to_read])) || ($fields[$field_to_read] < 0) 
		    || (($initialized) && ($baseflow > 0) && ($fields[$field_to_read] == 0))) {
		    next;
		}

		#initialize upon start up
		if ($initialized == 0) {
		    push @initialization, $fields[$field_to_read];
		    my $initsize = scalar(@initialization);
		    if ($initsize > 11) {
			$initialized = 1;
			$baseflow = average(@initialization);
			$latest_flow = $initialization[-1];
			my @initdevs;
			my $devmax = 0;
			for (my $t = 0; $t < $initsize-1; $t++) {
			    my $initdev = abs($initialization[$t] - $initialization[$t+1]);
			    push @initdevs, $initdev;
			    if ($initdev > $devmax) {
				$devmax = $initdev;
			    }
			}
			$basedeviation = average(@initdevs);
			print "Average initialization value: " . $baseflow . "\n";
			print "Average initialization deviation: " . $basedeviation . "\n";
			print "Max initialization deviation: " . $devmax . "\n";
			@initialization = ();
		    }
		    next;
		}

		my $latest_delta = $fields[$field_to_read]-$latest_flow;
		$latest_flow = $fields[$field_to_read];
		if ($initial_delta == 0) {
		    if ($maybe == 0) {
			if (($latest_delta > 3*$basedeviation) || (($latest_flow - $baseflow) > ($baseflow/5))) {
			    print "latest delta: " . $latest_delta . "\n";
			    print "latest flow: " . $latest_flow . "\n";
			    print "baseflow: " . $baseflow . "\n";
			    $maybe = 1;
			}
			push @flowarr, $fields[$field_to_read];
			push @timearr, $timefield;
			next;
		    }
		    if ((($latest_flow - $baseflow) > ($baseflow/5)) && ($latest_delta > 0)) {
			#still above base flow and going up for the second run in a row
			#first time flow was detected
			print "latest delta: " . $latest_delta . "\n";
			print "latest flow: " . $latest_flow . "\n";
			print "baseflow: " . $baseflow . "\n";
			print "initial flow detected\n";
			
			#update database
			my $dbh = DBI->connect($db, $db_user, $db_pass, { RaiseError => 1 })
			    or die $DBI::errstr;
			my $sth = $dbh->prepare("UPDATE outfalls SET AboveBaseFlow='true' WHERE Id = '${outfall_id}';");
			my $rv = $sth->execute
			    or warn $sth->errstr;
			$sth->finish;
			$dbh->disconnect;
			$initial_delta = 1;
			$maybe = 0;
			push @flowarr, $fields[$field_to_read];
			push @timearr, $timefield;
			next;
		    }
		    else {
			$maybe = 0;
			@timearr = ();
			@flowarr = ();
			next;
		    }
		}

		#flow has already been detected
		push @delta_ts, $latest_delta;
		push @timestamps, $timefield;
		push @flowarr, $fields[$field_to_read];
		#my $tmpepoch = timestringToEpoch($timefield);
		#my $tmptmstr = epochTo_yyyymmdd_hhMM($tmpepoch);
		push @timearr, $timefield;

		if ($waiting) {
		    #if waiting... 
		    #a) we already have an event
		    #b) we want to check permitted gauge for rainfall... reset if .1 has fallen in the waiting period
		    #c) if we haven't reached the waiting period we continue with the loop
		    my $possible_event_end_epoch = timestringToEpoch($timestamps[0]);				

		    #-2 and -1 would be most recent, but well step back a time step to account for latent arriving data 
		    my $lastEpoch = timestringToEpoch($timestamps[-3]);
		    my $nowEpoch = timestringToEpoch($timestamps[-2]);

		    print "last: " . $timestamps[-3] . " now: " . $timestamps[-2] . "\n";
		    $waitingPeriodGaugeTotal += get_permit_data($lastEpoch, $nowEpoch);
		    print "Waiting Period Gauge Total: " . $waitingPeriodGaugeTotal . "\n";
		    if ($waitingPeriodGaugeTotal > .1) {
			print "seems like a new event has occurred before the waiting period is up.  Restart\n";
			my $dbh = DBI->connect($db, $db_user, $db_pass, { RaiseError => 1 })
			    or die $DBI::errstr;

			my $sth = $dbh->prepare("UPDATE outfalls SET AboveBaseFlow=null WHERE Id = '${outfall_id}';");
			my $rv = $sth->execute
			    or warn $sth->errstr;
			$sth->finish;

			$sth = $dbh->prepare("SELECT EventId from outfalls WHERE Id = '${outfall_id}';");
			$rv = $sth->execute
			    or warn $sth->errstr;
			my $eventid = $sth->fetchrow();
			$sth->finish;

			my $rainstr = "rain during waiting period";
			$sth = $dbh->prepare("UPDATE events SET outcome = ? WHERE eventId = '${eventid}';");
			$sth->bind_param(1, $rainstr);
			$rv = $sth->execute
			    or warn $sth->errstr;
			$sth->finish;

			$sth = $dbh->prepare("UPDATE outfalls SET EventId=null WHERE Id = '${outfall_id}';");
			$rv = $sth->execute
			    or warn $sth->errstr;
			$sth->finish;

			$sth = $dbh->prepare("UPDATE outfalls SET Waiting72hrs='false' WHERE Id = '${outfall_id}';");
			$rv = $sth->execute
			    or warn $sth->errstr;
			$sth->finish;

			$sth = $dbh->prepare("UPDATE outfalls SET AccumulatingRainfall='false' WHERE Id = '${outfall_id}';");
			$rv = $sth->execute
			    or warn $sth->errstr;
			$sth->finish;

			$dbh->disconnect;
			
			$waiting = 0;
			$initialized = 0;
			@flowarr = ();
			@timearr = ();
			@timestamps = ();
			@permitvals = ();
			@permittimes = ();
			@delta_ts = ();
			$initial_delta = 0;
			$waitingPeriod_gaugeTotal = 0;
			next;
		    }
		    else {
			my $waittime = $nowEpoch - $possible_event_end_epoch;
			print "wait time: " . $waittime . "\n";
			if ($waittime < 259200) {
			    #if less than 72 hours, continue
			    next;
			}
			else {
			    print "72 hours reached!\n";
			    print "Now: $timefield and event end time: $timestamps[0]\n";
			    my $dbh = DBI->connect($db, $db_user, $db_pass, { RaiseError => 1 })
				or die $DBI::errstr;
			    my $sth = $dbh->prepare("UPDATE outfalls SET Waiting72hrs='false' WHERE Id = '${outfall_id}';");
			    my $rv = $sth->execute
				or warn $sth->errstr;
			    $sth->finish;

			    $sth = $dbh->prepare("UPDATE outfalls SET AccumulatingRainfall='true' WHERE Id = '${outfall_id}';");
			    $rv = $sth->execute
				or warn $sth->errstr;
			    $sth->finish;

			    $sth = $dbh->prepare("SELECT EventId from outfalls WHERE Id = '${outfall_id}';");
			    $rv = $sth->execute
				or warn $sth->errstr;
			    my $eventid = $sth->fetchrow();
			    $sth->finish;

			    my $inittstamptz = timestringToTimestampTZ($timefield);
			    $sth = $dbh->prepare("UPDATE events SET AccumulateStartTime='${inittstamptz}' WHERE EventId = '${eventid}';");
			    $rv = $sth->execute
				or warn $sth->errstr;
			    $sth->finish;

			    $dbh->disconnect;

			    my $init_message = "The 72hr waiting period following a flow event at " . $loc . " has ended.  Starting accumulations.";
			    send_email($from, $to, $subject, $init_message);

			    #$startEpoch = timestringToEpoch($timefield);
			    $waiting = 0;
			    $accumulating = 1;
			    next;
			} #/else ... the 72 hr waiting period is up
		    } # /else ... the gauge total in the wairing period is less than the threshold
		} # /if waiting

		if ($accumulating) {

		    my $startEpoch = timestringToEpoch($timestamps[-4]);
		    print "startEpoch " . $startEpoch . "\n";

		    my $endEpoch = timestringToEpoch($timestamps[-3]);
		    print "endEpoch " . $endEpoch . "\n";

		    my $init_str = epochTo_yyyymmdd_hhMM($startEpoch);
		    my $end_str = epochTo_yyyymmdd_hhMM($endEpoch);

		    #my $nowEpoch = time() - 600; #give 10 minutes for QPE to be available
		    #print "nowEpoch " . $nowEpoch . "\n";
		    #my $now_str = epochTo_yyyymmdd_hhMM($nowEpoch);

		    my $initendp = "https://droc2.srh.noaa.gov/cgi-bin/precip_query.pl?start=" . $init_str . "&end=" . $end_str . "&lat=" . $lat . "&lon=" . $lon . "&loc=" . $loc;
		    print $initendp . "\n";
		    $runningTotal += getQPE($initendp);
		    print "QPE total: " . $runningTotal . "\n";
		    $runningGaugeTotal += get_permit_data($startEpoch, $endEpoch);

		    push @totarr, $runningTotal;
		    push @accumtimearr, $end_str;
		    push @gaugetotarr, $runningGaugeTotal;

		    if ($runningTotal > $threshold) {
			#for now just make an entry
			my $dbh = DBI->connect($db, $db_user, $db_pass, { RaiseError => 1 })
			    or die $DBI::errstr;

			$sth = $dbh->prepare("SELECT EventId from outfalls WHERE Id = '${outfall_id}';");
			$rv = $sth->execute
			    or warn $sth->errstr;
			my $eventid = $sth->fetchrow();
			$sth->finish;

			my $qpealert_tstamptz = yyyymmdd_hhMM_ToTimestampTZ($end_str);
			$sth = $dbh->prepare("UPDATE events SET CasaRainTime='${qpealert_tstamptz}' WHERE eventId = '${eventid}' AND CasaRainTime IS NULL;");
			$rv = $sth->execute
			    or warn $sth->errstr;
			$sth->finish;

			$dbh->disconnect;
		    }

		    if ($runningGaugeTotal > $threshold) {
			print "alert!\n";
			#make pngs

			#flow graph
			my @master = (
			    [@timearr],
			    [@flowarr]
			    );

			my $max = max @flowarr;
			my $yskip = int($max/4);
			my $graphtitle = "Flow Event Concluding at " . $init_str;
			my $graph = GD::Graph::lines->new(1000, 500);
			$graph->set(
			    x_label             => 'Time',
			    y_label             => 'CFS',
			    title               => $graphtitle,
			    y_max_value         => int($max + 1),
			    y_tick_number       => 10*int($max + 1),
			    y_long_ticks        => 1,
			    y_min_value         => 0,
			    b_margin            => 10,
			    x_label_skip        => 10,
			    x_label_position    => 1/2,
			    x_labels_vertical   => 1,
			    y_label_skip        => $yskip,
			    #y_all_ticks         => 1,
			    transparent         => 0,
			    line_width          => 2,
			    bgclr               => "white"
			    );
			$graph->set_title_font('/fonts/arial.ttf', 34);
			$graph->set_x_axis_font('/fonts/arial.ttf', 24);
			$graph->set_y_axis_font('/fonts/arial.ttf', 26);
			$graph->set_y_label_font('/fonts/arial.ttf', 26);
			$graph->set_text_clr(black);
			my $gd = $graph->plot(\@master) or die $graph->error;
			open(IMG, '>/home/elyons/perl/flow_event_' . $event_no . '.png') or die $!;
			binmode IMG;
			print IMG $gd->png;

			#qpe graph
			my @accummaster = (
			    [@accumtimearr],
			    [@totarr],
			    [@gaugetotarr]
			    );

			my $accummax = max @totarr;

			my $accumgraphtitle = "Rainfall Accumulation at " . $loc . " since " . $accumtimearr[0];
			my $accumgraph = GD::Graph::lines->new(1000, 500);
			my $timesize = scalar(@accumtimearr);
			my $skip;
			if ($timesize < 120) { 
			    $skip = 10;
			}
			elsif ($timesize < 240) {
			    $skip = 15;
			}
			elsif ($timesize < 600) {
			    $skip = 30;
			}
			else {
			    $skip = 60;
			}

			$accumgraph->set(
			    x_label             => 'Time',
			    y_label             => 'Inches',
			    title               => $accumgraphtitle,
			    y_max_value         => (int(10*($accummax + .1))/10),
			    y_tick_number       => 10*(int(10*($accummax + .1))),
			    y_long_ticks        => 1,
			    y_min_value         => 0,
			    b_margin            => 10,
			    x_label_skip        => $skip,
			    x_label_position    => 1/2,
			    x_labels_vertical   => 1,
			    y_all_ticks         => 1,
			    transparent         => 0,
			    line_width          => 2,
			    bgclr               => "white"
			    );
			$accumgraph->set_title_font('/fonts/arial.ttf', 34);
			$accumgraph->set_x_axis_font('/fonts/arial.ttf', 24);
			$accumgraph->set_y_axis_font('/fonts/arial.ttf', 26);
			$accumgraph->set_y_label_font('/fonts/arial.ttf', 26);
			$accumgraph->set_text_clr(black);
			my $accumgd = $accumgraph->plot(\@accummaster) or die $accumgraph->error;
			open(ACCUMIMG, '>/home/elyons/perl/qpe_event_' . $event_no . '.png') or die $!;
			binmode ACCUMIMG;
			print ACCUMIMG $accumgd->png;


			my $message = 'Following the flow event at ' . $loc . ' CASA has detected rainfall exceeding ' . $threshold . 'inches';
			send_email($from, $to, $subject, $message, $event_no);

			#update database
			my $dbh = DBI->connect($db, $db_user, $db_pass, { RaiseError => 1 })
			    or die $DBI::errstr;
			$sth = $dbh->prepare("UPDATE outfalls SET AccumulatingRainfall='false' WHERE Id = '${outfall_id}';");
			$rv = $sth->execute
			    or warn $sth->errstr;
			$sth->finish;

			$sth = $dbh->prepare("SELECT EventId from outfalls WHERE Id = '${outfall_id}';"); 
			$rv = $sth->execute
			    or warn $sth->errstr;
			my $eventid = $sth->fetchrow();
			$sth->finish;

			my $samplestr = "sample";
			$sth = $dbh->prepare("UPDATE events SET outcome = ? WHERE eventId = '${eventid}';");
			$sth->bind_param(1,$samplestr);
			$rv = $sth->execute
			    or warn $sth->errstr;
			$sth->finish;

			my $qpealert_tstamptz = yyyymmdd_hhMM_ToTimestampTZ($accumtimearr[-1]);
			$sth = $dbh->prepare("UPDATE events SET PermitGaugeRainTime='${qpealert_tstamptz}' WHERE eventId = '${eventid}';");
			$rv = $sth->execute
			    or warn $sth->errstr;
			$sth->finish;

			$sth = $dbh->prepare("UPDATE outfalls SET eventid=null WHERE Id = '${outfall_id}';");
			$rv = $sth->execute
			    or warn $sth->errstr;
			$sth->finish;

			$dbh->disconnect;

			#increment event
			$event_no++;

			#reset variables
			$initialized = 0;
			@flowarr = ();
			@timearr = ();
			@timestamps = ();
			@permitvals = ();
			@permittimes = ();
			@delta_ts = ();
			$initial_delta = 0;
			$waiting = 0;
			$waitingPeriodGaugeTotal = 0;
			$runningGaugeTotal = 0;
			$runningTotal = 0;
		    }
		    next;
		}
		
		my $nDelts = scalar @delta_ts;
		if ($nDelts > 12) {

		    my $recent_av_delta = average(@delta_ts);
		    if ((abs($recent_av_delta) < 2*$basedeviation) && ($latest_flow < 3*$baseflow)) {
			$waiting = 1;
		    }
		    else {
			shift @delta_ts;
			shift @timestamps;
			next;
		    }
		    
		    print "Possible new alertable event.... Waiting 72 hrs\n";

		    my $dbh = DBI->connect($db, $db_user, $db_pass, { RaiseError => 1 })
			or die $DBI::errstr;

		    my $sth = $dbh->prepare("INSERT into events(outfallid) VALUES(${outfall_id});");
		    my $rv = $sth->execute
			or warn $sth->errstr;
		    $sth->finish;

		    $sth = $dbh->prepare("SELECT EventId FROM events WHERE outfallid = '${outfall_id}' ORDER BY EventId DESC NULLS LAST;");
		    $rv = $sth->execute
			or warn $sth->errstr;
		    my $eventid = $sth->fetchrow();
		    $sth->finish;

		    my $ongoingstr = "ongoing";
		    $sth = $dbh->prepare("UPDATE events SET outcome = ? WHERE EventId = '${eventid}';");
		    $sth->bind_param(1,$ongoingstr);
		    $rv = $sth->execute
			or warn $sth->errstr;
		    $sth->finish;

		    $sth = $dbh->prepare("UPDATE outfalls SET EventId='${eventid}' WHERE Id = '${outfall_id}';");
		    $rv = $sth->execute
			or warn $sth->errstr;
		    $sth->finish;

		    $sth = $dbh->prepare("UPDATE outfalls SET Waiting72hrs='true' WHERE Id = '${outfall_id}';");
		    $rv = $sth->execute
			or warn $sth->errstr;
		    $sth->finish;

		    $sth = $dbh->prepare("UPDATE outfalls SET AboveBaseFlow='false' WHERE Id = '${outfall_id}';");
		    $rv = $sth->execute
			or warn $sth->errstr;
		    $sth->finish;

		    my $bflow;
		    if ($flowarr[-1] eq "\"NAN\"") {
			$bflow = 0;
		    }
		    else {
			$bflow = $flowarr[-1];
		    }
		    $sth = $dbh->prepare("UPDATE outfalls SET BaseFlow='${bflow}' WHERE Id = '${outfall_id}';");
		    $rv = $sth->execute
			or warn $sth->errstr;
		    $sth->finish;

		    $sth = $dbh->prepare("UPDATE events SET BaseFlow='${bflow}' WHERE EventId = '${eventid}';");
		    $rv = $sth->execute
			or warn $sth->errstr;
		    $sth->finish;

		    my $starttstamptz = timestringToTimestampTZ($timearr[0]);
		    $sth = $dbh->prepare("UPDATE events SET FlowStartTime='${starttstamptz}' WHERE EventId = '${eventid}';");
		    $rv = $sth->execute
			or warn $sth->errstr;
		    $sth->finish;

		    my $endtstamptz = timestringToTimestampTZ($timefield);
		    $sth = $dbh->prepare("UPDATE events SET FlowEndTime='${endtstamptz}' WHERE EventId = '${eventid}';");
		    $rv = $sth->execute
				or warn $sth->errstr;
		    $sth->finish;

		    $dbh->disconnect;

		}
		next;   
	    }
	    
	    #mv the file out of the incoming directory
	    $file =~ s/ /\\ /g;
	    print $file;
	    system("rm -f $file");
	}
    }
}


sub daemonize {
    chdir '/'                 or die "Can't chdir to /: $!";
    open STDIN, '/dev/null'   or die "Can't read /dev/null: $!";
    open STDOUT, '>>/dev/null' or die "Can't write to /dev/null: $!";
    open STDERR, '>>/dev/null' or die "Can't write to /dev/null: $!";
    defined(my $pid = fork)   or die "Can't fork: $!";
    exit if $pid;
    setsid                    or die "Can't start a new session: $!";
    umask 0;
}

sub command_line_parse {
    if (($#ARGV < 0) || ($#ARGV > 3)) {
	print "Usage:  outfall_mon.pl input_dir outfall_id field_to_monitor (1 or 2 or 4 ->  flow, velocity, or level) data_format (0 or 1 -> via sharepoint or ftp)\n";
	exit;
    }
    $input_data_dir = $ARGV[0];
    $outfall_id = $ARGV[1];
    $field_to_monitor = $ARGV[2];
    $data_format = $ARGV[3];
    if (($data_format != 0) && ($data_format != 1)) {
	print "bad dataformat.  Assuming sharepoint ie. 0 \n";
	$data_format = 0;
    }
    if (($field_to_monitor != 1) && ($field_to_monitor != 2) && ($field_to_monitor != 4)) {
	print "bad field.  Using flow, ie. 1 \n";
	$field_to_monitor = 1;
    }
    if ($data_format == 1) {
	$field_to_read = $field_to_monitor + 1;
    }
    else {
	$field_to_read = $field_to_monitor;
    }
    my @rdd = split(/ /, $input_data_dir);
    foreach $w (@rdd) {
	print "Will recursively monitor $w for incoming csv files\n";
    }
}

sub yyyymmdd_hhMM_ToEpoch {
    my $datestr = $_[0];
    my $yr = substr($datestr,0,4);
    my $mo = substr($datestr,4,2);
    my $dy = substr($datestr,6,2);
    my $hr = substr($datestr,9,2);
    my $mn = substr($datestr,11,2);
    my $ss = "00";
    #my $ss = substr($datestr,18,2);
    #print $yr . $mo . $dy . "-" . $hr . $mn . $ss . "\n";
    
    my $datedt = DateTime->new(year => $yr, month => $mo, day => $dy, hour => $hr, minute => $mn, second => $ss, nanosecond => 0, time_zone  => 'America/Chicago');
    my $epoch = $datedt->epoch();
    #print "datestr " . $datedt->iso8601() . "\n";
    return($epoch);
}

sub timestringFormatSwap {
    my $timestring = $_[0];
    my @datestr_components = split(' ', $timestring);
    my @date_components = split('-', $datestr_components[0]);
    my $yr = substr($date_components[0], 1);
    my $mo = $date_components[1];
    my $dy = $date_components[2];
    my @time_components = split(':', $datestr_components[1]);
    my $hr = $time_components[0];
    my $mn = $time_components[1];
    my $outstr = $mo . "/" . $dy . "/" . $yr . " " . $hr . ":" . $mn;
    return $outstr;
}

sub timestringToEpoch {
    my $timestring = $_[0];
    my @datestr_components = split(' ', $timestring);
    my @date_components = split('/', $datestr_components[0]);
    my $yr = $date_components[2];
    my $mo = $date_components[0];
    my $dy = $date_components[1];
    my @time_components = split(':', $datestr_components[1]);
    my $hr = $time_components[0];
    my $mn = $time_components[1];
    my $ss = "00";
    my $datedt = DateTime->new(year => $yr, month => $mo, day => $dy, hour => $hr, minute => $mn, second => $ss, nanosecond => 0, time_zone  => 'America/Chicago');
    my $epoch = $datedt->epoch();
    #print "datestr " . $datedt->iso8601() . "\n";
    return($epoch);
}

sub timestringToTimestampTZ {
    my $timestring = $_[0];
    my @datestr_components = split(' ', $timestring);
    my @date_components = split('/', $datestr_components[0]);
    my $yr = $date_components[2];
    my $mo = $date_components[0];
    my $dy = $date_components[1];
    my @time_components = split(':', $datestr_components[1]);
    my $hr = $time_components[0];
    my $mn = $time_components[1];
    my $ss = "00";
    my $datedt = DateTime->new(year => $yr, month => $mo, day => $dy, hour => $hr, minute => $mn, second => $ss, nanosecond => 0, time_zone  => 'America/Chicago');
    my $tzsuffix;
    if ($datedt->is_dst()) {
	$tzsuffix = "-05";
    }
    else {
	$tzsuffix = "-06";
    }
    my $timestamptz = $yr . "-" . $mo . "-" . $dy . " " . $hr . ":" . $mn . ":" . $ss . $tzsuffix;
    #my $epoch = $datedt->epoch();
    #print "datestr " . $datedt->iso8601() . "\n";
    #return($datedt->iso8601());
    return($timestamptz);
}

sub epochTo_yyyymmdd_hhMM {
    my $epoch = $_[0];
    my ($sec, $min, $hour, $day, $month, $year) = (localtime($epoch))[0,1,2,3,4,5];
    my $yyyymmdd_hhMM = sprintf '%04d%02d%02d-%02d%02d', $year + 1900, $month + 1, $day, $hour, $min;
    #print "yyyymmdd_hhMM : " . $yyyymmdd_hhMM . "\n";
    return $yyyymmdd_hhMM;	
}

sub yyyymmdd_hhMM_ToTimestampTZ {
    my $timestring = $_[0];
    my $yr = substr($timestring, 0,4);
    my $mo = substr($timestring, 4,2);
    my $dy = substr($timestring, 6,2);
    my $hr = substr($timestring, 9,2);
    my $mn = substr($timestring, 11,2);
    my $ss = "00";
    my $datedt = DateTime->new(year => $yr, month => $mo, day => $dy, hour => $hr, minute => $mn, second => $ss, nanosecond => 0, time_zone  => 'America/Chicago');
    my $tzsuffix;
    if ($datedt->is_dst()) {
	$tzsuffix = "-05";
    }
    else {
	$tzsuffix = "-06";
    }
    my $timestamptz = $yr . "-" . $mo . "-" . $dy . " " . $hr . ":" . $mn . ":" . $ss . $tzsuffix;
    return($timestamptz);
}

sub getQPE {
    my $endpoint = $_[0];
    my $curl = WWW::Curl::Easy->new;
    $curl->setopt(CURLOPT_HEADER,1);
    $curl->setopt(CURLOPT_URL, $endpoint);
    my $response_body;
    $curl->setopt(CURLOPT_WRITEDATA,\$response_body);
    my $retcode = $curl->perform;
    
    if ($retcode == 0) {
	my $response_code = $curl->getinfo(CURLINFO_HTTP_CODE);
	my ($head,$body) = split( m{\r?\n\r?\n}, $response_body);
	return $body;
    }
    else {
	print("Error: $retcode ".$curl->strerror($retcode)." ".$curl->errbuf."\n");
	return;
    }
}

sub get_outfall_ll {
    my $geojsonfile = $_[0];
    open(FIL, $geojsonfile) or die("$!");
    read FIL, my $jsonstring, -s FIL;
    my $obj = Geo::JSON->from_json( $jsonstring );
    print $obj->type . "\n";
    #my @harr = ($obj->features)[0][0];
    my $numfeats = scalar @{$obj->features};
    for (my $featno = 0; $featno < $numfeats; $featno++ ) {
	my $feat = ($obj->features)[0][$featno];
	my @proparr = $feat->properties;
	my $OF_id = $proparr[0]{OUTFALL};
	if ($OF_id == $outfall_id) {
	    my $featgeo = $feat->geometry;
	    my @coords = $featgeo->coordinates;
	    $lon = $coords[0][0];
	    $lat = $coords[0][1];
	    $featno = $numfeats;
	}
    }
    print "lat: " . $lat . " lon: " . $lon . "\n";
}

sub send_email {
    my ( $email_from, $email_to, $email_subject, $email_message, $event_no ) = @_;
    
    my $eml = MIME::Lite->new ( From => $email_from, To => $email_to, Subject => $email_subject, Type => 'multipart/mixed');
    $eml->attach(Type => 'text', Data => $email_message);
    if (defined $event_no) {
	$eml->attach(Type => 'image/png', Path => '/home/elyons/perl/flow_event_' . $event_no . '.png',
		     Filename => 'flow_event_' . $event_no . '.png', Disposition => 'attachment' );
	$eml->attach(Type => 'image/png', Path => '/home/elyons/perl/qpe_event_' . $event_no . '.png',
		     Filename => 'qpe_event_' . $event_no . '.png', Disposition => 'attachment' );
    }
    $eml->send;
}

sub get_permit_data{
    my $start_epoch = $_[0];
    my $end_epoch = $_[1];
    my $apikey = "aeviyukrsmi1yafiucfflf2elmymrqha";
    my $apisecret = "myi3edklxeym6px9kbmro4blseygfjwj";
    my $stationid = "37870";
    my $t = time();
    my $authdata = "api-key" . $apikey . "end-timestamp" . $end_epoch . "start-timestamp" . $start_epoch . "station-id" . $stationid . "t" . $t;
    my $apisig = hmac_sha256_hex(encode("utf-8", $authdata), encode("utf-8", $apisecret));
    my $wlinkurl = "https://api.weatherlink.com/v2/historic/" . $stationid . "?api-key=" . $apikey . "&t=" . $t . "&start-timestamp=" . $start_epoch . "&end-timestamp=" . $end_epoch . "&api-signature=" . $apisig;
    my $getcli = REST::Client->new();
    $getcli->GET($wlinkurl);
    my $jscalar = from_json($getcli->responseContent());
    #print $jscalar . " jscalar " . $getcli->responseContent() . " getcli\n";
    my @objects = keys %jscalar;
    #foreach my $key (@objects) {
	#print $key . "\n";
    #}
    my @dataarray = @{$jscalar->{'sensors'}[0]->{'data'}};
    my $datalen = scalar(@dataarray);
    my $intervalAccum = 0;
    
    if ($datalen < 1) {
	$gauge_online = 0;
    }
    else {
	$gauge_online = 1;
	my @gaugeAccums;
	for(my $c=0; $c < $datalen; $c++) {
	    if (looks_like_number($jscalar->{'sensors'}[0]->{'data'}[$c]->{'ts'})) {
		my $utc_tstring = DateTime->from_epoch(epoch => $jscalar->{'sensors'}[0]->{'data'}[$c]->{'ts'});
		$utc_tstring->set_time_zone('America/Chicago');
		push @permittimes, $utc_tstring->iso8601();
	    }
	    if (looks_like_number($jscalar->{'sensors'}[0]->{'data'}[$c]->{'rainfall_in'})) {
		push @permitvals, $jscalar->{'sensors'}[0]->{'data'}[$c]->{'rainfall_in'};
		push @gaugeAccums, $jscalar->{'sensors'}[0]->{'data'}[$c]->{'rainfall_in'};
	    }
	}
	foreach my $accum (@gaugeAccums) {
	    $intervalAccum = $intervalAccum + $accum;
	}
    }
    print "Gauge total is: " . $intervalAccum . "\n";
    return $intervalAccum;
}

sub average {
    my @array = @_; 
    my $sum;
    foreach (@array) { $sum += $_; } 
    return $sum/@array; 
}
