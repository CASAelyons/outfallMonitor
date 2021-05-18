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
use List::Util qw( min max );
use WWW::Curl::Easy;
use MIME::Lite;
use Geo::JSON;

our $input_data_dir;
our $outfall_id;
our $field_to_monitor;
&command_line_parse;

#&daemonize;

our @delta_ts;
our $latest_flow = -1;
our @timestamps;
our $accumulating = 0;
our $waiting = 0;
our $initial_delta = 0;
our @flowarr;
our @timearr;
our $event_created = 0;
our $event_no = 0;

our $lat;# = 32.9199;
our $lon;# = -97.0335;
our $loc = "Outfall_" . $outfall_id; #059";
our $outfall_geojson_fn = "/home/elyons/perl/dfwairport_outfalls.geojson";
our $possible_endtime;

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
	sleep 60;
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
	    else {

		print "opening Outfall \n";
		open(my $data, '<', $file) or die "Could not open '$file' $!\n";
		    
		while (my $line = <$data>) {
		    print "line: " . $line . "\n";
		    my @fields = split "," , $line;
		    my $tstamptz = timestringToTimestampTZ($fields[0]);

		    my $dbh = DBI->connect($db, $db_user, $db_pass, { RaiseError => 1 })
			or die $DBI::errstr;

		    my $sth = $dbh->prepare("UPDATE outfalls SET lastData='${tstamptz}' WHERE Id = '${outfall_id}';");
		    my $rv = $sth->execute
			or warn $sth->errstr;
		    $sth->finish;
		    $dbh->disconnect;
			    
		    #error check
		    if ($fields[$field_to_monitor] < 0) {
			next;
		    }
		    #initial line upon start up
		    if ($latest_flow == -1) {
			$latest_flow = $fields[$field_to_monitor];
			next;
		    }
		    else {
			my $latest_delta = abs($fields[$field_to_monitor]-$latest_flow);
			$latest_flow = $fields[$field_to_monitor];
			if ($initial_delta == 0) {
			    if ($latest_delta > .1) {
				#first time flow was detected
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
				push @flowarr, $fields[$field_to_monitor];
				my $tmpepoch = timestringToEpoch($fields[0]);
				my $tmptmstr = epochTo_yyyymmdd_hhMM($tmpepoch);
				push @timearr, $tmptmstr;
				
				#print @flowarr[0] . "\n";
			    }
			    next;
			}
			else {
			    #flow has already been detected
			    push @delta_ts, $latest_delta;
			    push @timestamps, $fields[0];
			    push @flowarr, $fields[$field_to_monitor];
			    my $tmpepoch = timestringToEpoch($fields[0]);
			    my $tmptmstr = epochTo_yyyymmdd_hhMM($tmpepoch);
			    push @timearr, $tmptmstr;
			    
			    my $nDelts = scalar @delta_ts;
			    if ($nDelts > 6) {
				my $steady = 1;
				my $num_fluctuations = 0;
				for (my $t = 0; $t < $nDelts; $t++) {
				    if ($delta_ts[$t] > .1) {
					$steady = 0;
					$num_fluctuations++;
				    }
				}
				if ($steady == 0) {
				    shift @delta_ts;
				    shift @timestamps;
				    if ($num_fluctuations > 3) {					
					if (($waiting) && (!$accumulating)) {
					    print "seems like a new event has occurred before the waiting period is up.  Restart\n"; 

					    my $dbh = DBI->connect($db, $db_user, $db_pass, { RaiseError => 1 })
						or die $DBI::errstr;

					    #set abovebaseflow and eventid to null for now... we may well be above base flow but we're starting over
					    my $sth = $dbh->prepare("UPDATE outfalls SET AboveBaseFlow=null WHERE Id = '${outfall_id}';");
					    my $rv = $sth->execute
						or warn $sth->errstr;
					    $sth->finish;

					    $sth = $dbh->prepare("SELECT EventId from outfalls WHERE Id = '${outfall_id}';");
					    $rv = $sth->execute
						or warn $sth->errstr;
					    my $eventid = $sth->fetchrow();
					    $sth->finish;

					    $sth = $dbh->prepare("UPDATE events SET eventsampled='false' WHERE eventId = '${eventid}';");
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
					    $event_created = 0;
					    @flowarr = ();
					    @timearr = ();
					    @timestamps = ();
					    @delta_ts = ();
					    $initial_delta = 0;
					}
				    }
					
				    next;
				} 
				else {
				    #here marks a possible end of event... now have to wait 72 hours... restarting if there are any new events in the interim
				    #kill any previously running monitoring loops and start from scratch
				    if ($waiting == 0) {
					print "possible end of alertable event... waiting 72 hours\n";
					if ($event_created == 0) {
					    my $dbh = DBI->connect($db, $db_user, $db_pass, { RaiseError => 1 })
						or die $DBI::errstr;
					    
					    my $sth = $dbh->prepare("INSERT into events(outfallid, eventsampled, producttype) VALUES(${outfall_id}, false, ${field_to_monitor});");
					    my $rv = $sth->execute
						or warn $sth->errstr;
					    $sth->finish;
					    
					    $sth = $dbh->prepare("SELECT EventId FROM events WHERE outfallid = '${outfall_id}' ORDER BY EventId DESC NULLS LAST;");
					    $rv = $sth->execute
						or warn $sth->errstr;
					    my $eventid = $sth->fetchrow();
					    $sth->finish;
					    
					    $sth = $dbh->prepare("UPDATE outfalls set EventId='${eventid}' WHERE Id = '${outfall_id}';");
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

					    my $starttstamptz = yyyymmdd_hhMM_ToTimestampTZ($timearr[0]);
					    $sth = $dbh->prepare("UPDATE events SET FlowStartTime='${starttstamptz}' WHERE EventId = '${eventid}';");
					    $rv = $sth->execute
						or warn $sth->errstr;
					    $sth->finish;

					    my $endtstamptz = timestringToTimestampTZ($fields[0]);
					    $sth = $dbh->prepare("UPDATE events SET FlowEndTime='${endtstamptz}' WHERE EventId = '${eventid}';");
					    $rv = $sth->execute
						or warn $sth->errstr;
					    $sth->finish;
					    
					    $dbh->disconnect;
					}
					$waiting = 1;
				    }

				    my $nowepoch = timestringToEpoch($fields[0]);
				    my $possible_event_end_epoch = timestringToEpoch($timestamps[0]);
				    my $tmpendts = epochTo_yyyymmdd_hhMM($possible_event_end_epoch);
				    my $endUTC = yyyymmdd_hhMM_ToEpoch($tmpendts);
				    my $possible_event_start_epoch = yyyymmdd_hhMM_ToEpoch($timearr[0]);
				    
				    my $waittime = $nowepoch - $possible_event_end_epoch;
				    my $event_length = $endUTC - $possible_event_start_epoch;
				    #print "first time: " . $timearr[0] . "\n";
				    #print "start?: " . $possible_event_start_epoch . "\n";
				    #print "end?: " . $possible_event_end_epoch . "\n";
				    print "wait time: " . $waittime . "\n";
				    #print "event_length: " . $event_length . "\n";
				    #if the event hardly lasted any time at all, it was probably bad data
				    #cancel it and move on
				    
				    if ($event_length < 2000) {
					print "false alarm we think.  Starting over\n";
					my $dbh = DBI->connect($db, $db_user, $db_pass, { RaiseError => 1 })
					    or die $DBI::errstr;

					#end existing events and go back to default
					my $sth = $dbh->prepare("UPDATE outfalls SET AboveBaseFlow=null WHERE Id = '${outfall_id}';");
					my $rv = $sth->execute
					    or warn $sth->errstr;
					$sth->finish;

					$sth = $dbh->prepare("SELECT EventId from outfalls WHERE Id = '${outfall_id}';");
					$rv = $sth->execute
					    or warn $sth->errstr;
					my $eventid = $sth->fetchrow();
					$sth->finish;

					$sth = $dbh->prepare("UPDATE events SET eventsampled='false' WHERE eventId = '${eventid}';");
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
					
					$initial_delta = 0;
					@flowarr = ();
					@timearr = ();
					@timestamps = ();
					@delta_ts = ();
					$waiting = 0;
					$event_created = 0;
					
					next;
				    }
				    
				    if ($waittime < 259200) {
					#if less than 72 hours, continue
					next;
				    }
				    else {
					print "72 hours reached!\n";
					print "Now: $fields[0] and event end time: $timestamps[0]\n";
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

					my $inittstamptz = timestringToTimestampTZ($fields[0]);
					$sth = $dbh->prepare("UPDATE events SET AccumulateStartTime='${inittstamptz}' WHERE EventId = '${eventid}';");
					$rv = $sth->execute
					    or warn $sth->errstr;
					$sth->finish;
					
					$dbh->disconnect;
					$waiting = 0;
					$accumulating = 1;
				    
					#for (my $t = 0; $t < $nDelts; $t++) {
					#    print $timestamps[$t] . " delta: " . $delta_ts[$t] . "\n";
					#}
				    
					#get precip
					my $threshold = .1;
					my $runningTotal = 0; 
					my @totarr;
					my @accumtimearr;
					push @totarr, 0;
					
				    
					my $startEpoch = timestringToEpoch($timestamps[0]);
					print "startEpoch " . $startEpoch . "\n";
					
					my $endEpoch = timestringToEpoch($timestamps[$ndelts - 1]);
					print "endEpoch " . $endEpoch . "\n";
					
					my $init_str = epochTo_yyyymmdd_hhMM($startEpoch);
					my $end_str = epochTo_yyyymmdd_hhMM($endEpoch);
					
					my $nowEpoch = time();
					print "nowEpoch " . $nowEpoch . "\n";
					my $now_str = epochTo_yyyymmdd_hhMM($nowEpoch);

				        my $initendp = "https://droc2.srh.noaa.gov/cgi-bin/precip_query.pl?start=" . $end_str . "&end=" .
					    $now_str . "&lat=" . $lat . "&lon=" . $lon . "&loc=" . $loc;
					print "initial query: " . $initendp . "\n";
					
					my $start_str = $now_str;
					$endEpoch = $nowEpoch;

					my $init_message = "The 72hr waiting period following a flow event at " . $loc . " has ended.  Starting accumulations.";
					send_email($from, $to, $subject, $init_message);
					#because the data may show up sporadically, the accumulation initiation could start in the past
					#therefore lets accumulate to the present before we start going minute by minute
					print "sleeping for 10 minutes to let the QPE catch up\n";
					sleep 600;
					print "done sleeping... QPE should be caught up.  Now we accumulate...\n";
					print "Getting any accumulations that occurred between the end of the 72 hr waiting period and now\n";
					$runningTotal += getQPE($initendp);
					push @totarr, $runningTotal;
					push @accumtimearr, $end_str;
					print "Total in the interim: " . $runningTotal . "\n";
					if ($runningTotal >= $threshold) {
					    print "Exceeded our threshold in the interim.\n";
					    $accumulating = 0;
					}
					    
					while($accumulating == 1){
					    if ($runningTotal < $threshold) {
						print "threshold: " . $threshold . " rt: " . $runningTotal . "\n";
						$endEpoch += 60;
						$end_str = epochTo_yyyymmdd_hhMM($endEpoch);
						push @accumtimearr, $end_str;
						my $endp = "https://droc2.srh.noaa.gov/cgi-bin/precip_query.pl?start=" . $start_str . "&end=" .
						    $end_str . "&lat=" . $lat . "&lon=" . $lon . "&loc=" . $loc;
						print "Endpoint $endp \n";
						$runningTotal += getQPE($endp);
						push @totarr, $runningTotal;
						$start_str = $end_str;
						sleep(60);
					    }
					    else {
						$accumulating = 0;
					    }
					}
				    
				    
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
					    [@totarr]
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
					
					#send email
					#my $to = 'elyons19@hotmail.com, elyons@engin.umass.edu, chughes@dfwairport.com, stan1@dfwairport.com, aackel1@dfwairport.com, alexisackel@gmail.com, samgotan@gmail.com';

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
					
					$sth = $dbh->prepare("UPDATE events SET eventsampled='true' WHERE eventId = '${eventid}';");
					$rv = $sth->execute
					    or warn $sth->errstr;
					$sth->finish;
					
					my $qpealert_tstamptz = yyyymmdd_hhMM_ToTimestampTZ($accumtimearr[-1]);
					$sth = $dbh->prepare("UPDATE events SET CasaRainTime='${qpealert_tstamptz}' WHERE eventId = '${eventid}';");
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
					@flowarr = ();
					@timearr = ();
					@timestamps = ();
					@delta_ts = ();
					$initial_delta = 0;
					$waiting = 0;
					$event_created = 0;
				    }
				}
			    }
			}
		    }
		}
		#mv the file out of the incoming directory
		$file =~ s/ /\\ /g;
		print $file;
		system("rm $file");
	    }
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
    if (($#ARGV < 0) || ($#ARGV > 2)) {
	print "Usage:  outfall_mon.pl input_dir outfall_id field_to_monitor (1 or 2) \n";
	exit;
    }
    $input_data_dir = $ARGV[0];
    $outfall_id = $ARGV[1];
    $field_to_monitor = $ARGV[2];
    if (($field_to_monitor != 1) && ($field_to_monitor != 2)) {
	print "bad field.  Using flow, ie. 1 \n";
	$field_to_monitor = 1;
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
