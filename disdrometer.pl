#!/usr/bin/perl -w

##################################################################################################
#####WRITTEN BY ERIC LYONS 1/2022 on behalf of CASA, TxSense LLC##########################
##################################################################################################
#  TESTED FUNCTIONALITY:                                                                         #
#  
#   -takes web query for start and end times with CGI perl
#   -creates a disdrometer timeseries over interval
#   -Generates graph
#  
##################################################################################################
use strict;
use warnings;

use POSIX qw(setsid);
use Scalar::Util qw(looks_like_number);
use File::Path;
use File::Copy;
use DateTime;
use CGI;
use GD::Graph;
use GD::Graph::lines;
use Fcntl;
use lib "/home/elyons/perl";

our $DATA_LOCATION = "/data/disdrometer";

our $START_TIME; #START_TIME given on command line
our $END_TIME; #END_TIME given on command line
our $OUTPUT_DIR = "/var/www/html/disdrometer/";
our $start_epoch;
our $end_epoch;
our $start_ymd;
our $end_ymd;
our $start_hh;
our $end_hh;
our $start_mm;
our $end_mm;
our $sm1;
our $sm2;
our $em1;
our $em2;
our $mid_m1;
our $mid_hh;
our $this_ymd;
our $this_hh;
our $startfile_ymdhms;
our @filelist;
our @times;
our @accums;
our @totals;
our $total = 0;
our $pngname;
our $outpngname;
our $params;
&command_line_parse;
&create_file_list;

create_disdrometer_time_series( @filelist );
&graph_data;
#my $outURL ="https://emmy10.casa.umass.edu/disdrometer/" . $pngname;
print $params -> header(
    #-type => 'text/html',
    -type => 'image/png',
    -access_control_allow_origin => '*',
    -access_control_allow_headers => 'content-type,X-Requested-With',
    -access_control_allow_methods => 'GET,OPTIONS',
    );

select(STDOUT); $| = 1;   #unbuffer STDOUT
#print "Content-type: image/png\n\n";

open (IMAGE, '<', $outpngname);
print <IMAGE>;
close IMAGE;
#our $outURL = "test";
#print $outURL;
#print "test";
exit;

sub create_disdrometer_time_series {
    foreach my $file (@filelist) {
	&extract_accum_and_time($file);
    }
}

sub extract_accum_and_time {
    my $fname = shift;

    #get time
    my $eyyyy = substr $fname, -19, 4;
    my $emo = substr $fname, -15, 2;
    my $edy = substr $fname, -13, 2;
    my $ehh = substr $fname, -10, 2;
    my $emi = substr $fname, -8, 2;
    my $ems = substr $fname, -6, 2;
    my $edt = DateTime->new( year => $eyyyy, month => $emo, day => $edy, hour => $ehh, minute => $emi, second => $ems, time_zone => "UTC");
    $edt->set_time_zone('America/Chicago');
    my $outtmstr = $edt->strftime("%Y%m%d-%H%M%S");
    push @times, $outtmstr;

    #get accum
    open my $fh, '<', $fname or die "$fname: $!";
    my $line;
    while( <$fh> ) {
	if( $. == 2 ) {
	    $line = $_;
	    last;
	}
    }
    my @valspl = split(':', $line);
    my $accumval = $valspl[1];
    my $accumval_inches = ($accumval * .03937) / 60; #convert mm/hr to inches
    push @accums, $accumval_inches;
    $total = $total + $accumval_inches;
    push @totals, $total;
}

sub graph_data {
    #make a plot
    my $total_abbrev = sprintf("%.3f", $totals[-1]);
    my $plottitle = "SE HoldPad Rainfall Timeseries  (total = " . $total_abbrev . " in)";
    #my $subtitle = "Total accumulation: " . $totals[-1] . " in\n";
    my $graph = GD::Graph::lines->new(1500,750);

    $graph->set(
	x_label             => 'Time',
	y_label             => 'Inches',
	title               => $plottitle,
	text_space          => 15,
	#subtitle            => $subtitle,
	y_max_value         => int($totals[-1] + 1),
	y_tick_number       => 10*int($totals[-1] + 1),
	y_long_ticks        => 1,
	y_min_value         => 0,
	t_margin            => 10,
	b_margin            => 10,
	r_margin            => 10,
	l_margin            => 10,
	x_label_skip        => 15,
	x_label_position    => 1/2,
	x_labels_vertical   => 1,
	legend_placement    => 'RC',
	borderclrs          => [undef],
	boxclr              => "white",
	bgclr               => "#fffd48",
	fgclr               => '#bbbbbb',
	axislabelclr        => '#333333',
	labelclr            => '#333333',
	textclr             => '#333333',
	legendclr           => '#333333',
	y_all_ticks         => 1,
	transparent         => 0,
	line_width          => 2
	);

    my @series = ("Rainfall");

    
    #set the fonts
    #my $fontloc = "/usr/share/fonts/truetype/freefont/FreeSans.ttf";
    #my $font = "/usr/share/fonts/truetype/liberation/LiberationSerif-Regular.ttf";
    my $fontloc = '/usr/share/fonts/truetype/freefont/FreeSans.ttf';
    $graph->set_title_font($fontloc, 16);
    $graph->set_x_label_font($fontloc, 12);
    $graph->set_y_label_font($fontloc, 12);
    $graph->set_x_axis_font($fontloc, 10);
    $graph->set_y_axis_font($fontloc, 10);
    $graph->set_legend_font($fontloc, 10);
    
    #set the legend
    $graph->set_legend ( @series );

    #format the time
    my @formatted_times;
    for my $thistime(@times) {
	my $hh_1 = substr($thistime, -6, 2);
	my $mm_1 = substr($thistime, -4, 2);
	my $yyyy_1 = substr($thistime, 0, 4);
	my $mo_1 = substr($thistime, 4, 2);
	my $dd_1 = substr($thistime, 6, 2);
	my $formatted_time = $mo_1 . "/" . $dd_1 . "/" . $yyyy_1 . " " . $hh_1 . ":" . $mm_1;
	push(@formatted_times, $formatted_time);
    }
    
    #set the data
    my @dataset = (
	[ @formatted_times ],
	[ @totals ]
	);
    
    #make the plot
    $pngname = "disdrometer_query_" . $times[0] . "_" . $times[-1] . ".png";
    $outpngname = $OUTPUT_DIR . $pngname;
    my $gd = $graph->plot(\@dataset) or die $graph->error;
    open(IMG, '>', $outpngname) or die $!;
    binmode IMG;
    print IMG $gd->png;
}

sub create_file_list {
    if ($start_ymd == $end_ymd) {
	if ($start_hh == $end_hh) {
	    if ($sm1 == $em1) {
		&get_minutes_only;
	    }
	    else {
		&get_start_minutes;
		&get_end_minutes;
		$mid_m1 = $sm1 + 1;
		$this_ymd = $start_ymd;
		$this_hh = $start_hh;
		while ($mid_m1 != $em1) {
		    &get_mid_minutes;
		    $mid_m1++;
		}
	    }
	}
	else {
	    &get_start_hour;
	    &get_end_hour;
	    $mid_hh = $start_hh + 1;
	    if ($mid_hh > 23) {
		$mid_hh = 0;
		$this_ymd = $end_ymd;
	    }
	    else {
		$this_ymd = $start_ymd;
	    }
	    while ($mid_hh != $end_hh) {
		&get_mid_hour;
		$mid_hh++;
		if ($mid_hh > 23) {
		    $mid_hh = 0;
		    $this_ymd = $end_ymd;
		}
	    }
	}	    
    }
    else {
	&get_start_hour;
	if ($start_hh < 23) {
	    $mid_hh = $start_hh + 1;
	    while ($mid_hh < 24) {
		&get_mid_hour;
		$mid_hh++;
	    }
	}
	&get_end_hour;
	if ($end_hh > 0) {
	    $mid_hh = 00;
	    while ($mid_hh != $end_hh) {
		&get_mid_hour;
		$mid_hh++;
	    }
	}
    }
    @filelist = sort @filelist;
    my $startfile_ymd = substr $filelist[0], -19, 8;
    my $startfile_hms = substr $filelist[0], -10, 6;
    $startfile_ymdhms = $startfile_ymd . "-" . $startfile_hms;
}

sub get_minutes_only {
    my $hhmm_pattern = $end_ymd . "_" . $end_hh . $em1 . "\[$sm2\-$em2\]";
    my $datadir = "$DATA_LOCATION/$end_ymd";
    opendir(DIR, $datadir);
    my @matching_files_in_dir = grep(/$hhmm_pattern/, readdir(DIR));
    closedir(DIR);
    foreach my $matching_file (@matching_files_in_dir) {
	my $match_path = $datadir . "/" . $matching_file;
	push @filelist, $match_path;
    }
}
    
sub get_end_minutes {
    my $em_pattern = $end_ymd . "_" . $end_hh . $em1 . "\[0\-$em2\]";
    my $datadir = "$DATA_LOCATION/$end_ymd";
    opendir(DIR, $datadir);
    my @matching_files_in_dir = grep(/$em_pattern/, readdir(DIR));
    closedir(DIR);
    foreach my $matching_file (@matching_files_in_dir) {
	my $match_path =$datadir . "/" . $matching_file;
        push @filelist, $match_path;
    }
}

sub get_start_minutes {
    my $sm_pattern = $start_ymd . "_" . $start_hh . $sm1 . "\[$sm2\-9\]";
    my $datadir = "$DATA_LOCATION/$start_ymd";
    opendir(DIR, $datadir);
    my @matching_files_in_dir = grep(/$sm_pattern/, readdir(DIR));
    closedir(DIR);
    foreach my $matching_file (@matching_files_in_dir) {
        my $match_path =$datadir . "/" . $matching_file;
        push @filelist, $match_path;
    }
}

sub get_mid_minutes {
    my $mid_pattern = $this_ymd . "_" . $this_hh . $mid_m1 . "\[0\-9\]";
    my $datadir = "$DATA_LOCATION/$this_ymd";
    opendir(DIR, $datadir);
    my @matching_files_in_dir = grep(/$mid_pattern/, readdir(DIR));
    closedir(DIR);
    foreach my $matching_file (@matching_files_in_dir) {
        my $match_path =$datadir . "/" . $matching_file;
        push @filelist, $match_path;
    }
}

sub get_end_hour {
    &get_end_minutes;
    $mid_m1 = $em1 - 1;
    $this_ymd = $end_ymd;
    $this_hh = $end_hh;
    while ($mid_m1 > -1) {
	&get_mid_minutes;
        $mid_m1--;
    }
}

sub get_start_hour {
    &get_start_minutes;
    $mid_m1 = $sm1 + 1;
    $this_ymd = $start_ymd;
    $this_hh = $start_hh;
    while ($mid_m1 < 6) {
	&get_mid_minutes;
	$mid_m1++;
    }
}

sub get_mid_hour {
    if ($mid_hh > 0) {
	$mid_hh =~ s/^0//;
    }
    if ($mid_hh < 10) {
	$mid_hh = "0" . $mid_hh;
    }
    my $midhr_pattern = $this_ymd . "_" . $mid_hh;
    my $datadir = "$DATA_LOCATION/$this_ymd";
    opendir(DIR, $datadir);
    my @matching_files_in_dir = grep(/$midhr_pattern/, readdir(DIR));
    closedir(DIR);
    foreach my $matching_file (@matching_files_in_dir) {
	my $match_path =$datadir . "/" . $matching_file;
        push @filelist, $match_path;
    }
}

sub command_line_parse {

    $params = CGI->new();
    $START_TIME = $params->param('start');
    $END_TIME = $params->param('end');

    if ( (not defined $START_TIME) || (not defined $END_TIME)) {
	print "Content-type: text/html\n\n";
	#print "Access-Control-Allow-Origin: *";
	print "<html>";
	print "<header><title>Usage FAIL</title></header>";
	print "<body>";
	print "Specify start and end parameters... exiting\n";
	print "</body>";
	print "</html>";
	exit;
    }

    $start_ymd = substr($START_TIME, 0, 8);
    $start_hh = substr($START_TIME, 9, 2);
    $start_mm = substr($START_TIME, 11, 2);
    $sm1 = substr($start_mm, 0, 1);
    $sm2 = substr($start_mm, 1,1);
    $end_ymd = substr($END_TIME, 0, 8);
    $end_hh = substr($END_TIME, 9, 2);
    $end_mm = substr($END_TIME, 11, 2);
    $em1 = substr($end_mm, 0,1);
    $em2 = substr($end_mm, 1,1);

    my $start_yyyy = substr($start_ymd, 0, 4);
    my $start_mo = substr($start_ymd, 4, 2);
    my $start_dd = substr($start_ymd, 6, 2);
        
    if (( !looks_like_number($start_yyyy)) || ( !looks_like_number($start_mo)) || (!looks_like_number($start_dd)) || ( !looks_like_number($start_hh)) || ( !looks_like_number($start_mm))){
	print "Content-type: text/html\n\n";
	#print "Access-Control-Allow-Origin: *";
	print "<html>";
	print "<header><title>Usage FAIL</title></header>";
	print "<body>";
	print "Enter numeric values for times... exiting\n";
	print "</body>";
	print "</html>";
	exit;
    }
    
    my $end_yyyy = substr($end_ymd, 0, 4);
    my $end_mo = substr($end_ymd, 4, 2);
    my $end_dd = substr($end_ymd, 6, 2);

    if (( !looks_like_number($end_yyyy)) || ( !looks_like_number($end_mo)) || (!looks_like_number($end_dd)) || ( !looks_like_number($end_hh)) || ( !looks_like_number($end_mm))){ 
        print "Content-type: text/html\n\n";
	#print "Access-Control-Allow-Origin: *";
	print "<html>";
	print "<header><title>Usage FAIL</title></header>";
	print "<body>";
	print "Enter numeric values for times... exiting\n";
	print "</body>";
	print "</html>";
	exit;
    }
    
    my $start_date = DateTime->new( year => $start_yyyy, month => $start_mo, day => $start_dd, hour => $start_hh, minute => $start_mm, second => 0, time_zone => "UTC");
    $start_epoch = $start_date->epoch;

    my $end_date = DateTime->new( year => $end_yyyy, month => $end_mo, day => $end_dd, hour => $end_hh, minute => $end_mm, second => 0, time_zone => "UTC");
    $end_epoch = $end_date->epoch;

    if (($start_yyyy < 2022) || ($start_yyyy > 2022)) {
	print "Content-type: text/html\n\n";
	#print "Access-Control-Allow-Origin: *";
	print "<html>";
	print "<header><title>Usage FAIL</title></header>";
	print "<body>";
	print "Start time out of range \n";
	print "</body>";
	print "</html>";
	exit;
    }

    if (($start_mo < 1) || ($start_mo > 12)) {
	print "Content-type: text/html\n\n";
	#print "Access-Control-Allow-Origin: *";
	print "<html>";
	print "<header><title>Usage FAIL</title></header>";
	print "<body>";
	print "Start time out of range \n";
	print "</body>";
	print "</html>";
	exit;
    }

    if (($start_dd < 1) || ($start_dd > 31)) {
	print "Content-type: text/html\n\n";
	#print "Access-Control-Allow-Origin: *";
	print "<html>";
	print "<header><title>Usage FAIL</title></header>";
	print "<body>";
	print "Start time out of range \n";
	print "</body>";
	print "</html>";
	exit;
    }

    if (($start_hh < 0) || ($start_hh > 23)) {
	print "Content-type: text/html\n\n";
	#print "Access-Control-Allow-Origin: *";
	print "<html>";
	print "<header><title>Usage FAIL</title></header>";
	print "<body>";
	print "Start time out of range \n";
	print "</body>";
	print "</html>";
	exit;
    }

    if (($start_mm < 0) || ($start_mm > 59)) {
	print "Content-type: text/html\n\n";
	#print "Access-Control-Allow-Origin: *";
	print "<html>";
	print "<header><title>Usage FAIL</title></header>";
	print "<body>";
	print "Start time out of range \n";
	print "</body>";
	print "</html>";
	exit;
    }

    if (($end_yyyy < 2022) || ($end_yyyy > 2022)) {
	print "Content-type: text/html\n\n";
	#print "Access-Control-Allow-Origin: *";
	print "<html>";
	print "<header><title>Usage FAIL</title></header>";
	print "<body>";
	print "End time out of range \n";
	print "</body>";
	print "</html>";
	exit;
    }

    if (($end_mo < 1) || ($end_mo > 12)) {
	print "Content-type: text/html\n\n";
	#print "Access-Control-Allow-Origin: *";
	print "<html>";
	print "<header><title>Usage FAIL</title></header>";
	print "<body>";
	print "End time out of range \n";
	print "</body>";
	print "</html>";
	exit;
    }

    if (($end_dd < 1) || ($end_dd > 31)) {
	print "Content-type: text/html\n\n";
	#print "Access-Control-Allow-Origin: *";
	print "<html>";
	print "<header><title>Usage FAIL</title></header>";
	print "<body>";
	print "End time out of range \n";
	print "</body>";
	print "</html>";
	exit;
    }

    if (($end_hh < 0) || ($end_hh > 23)) {
	print "Content-type: text/html\n\n";
	#print "Access-Control-Allow-Origin: *";
	print "<html>";
	print "<header><title>Usage FAIL</title></header>";
	print "<body>";
	print "End time out of range \n";
	print "</body>";
	print "</html>";
	exit;
    }

    if (($end_mm < 0) || ($end_mm > 59)) {
	print "Content-type: text/html\n\n";
	#print "Access-Control-Allow-Origin: *";
	print "<html>";
	print "<header><title>Usage FAIL</title></header>";
	print "<body>";
	print "End time out of range \n";
	print "</body>";
	print "</html>";
	exit;
    }

    if ($start_ymd > $end_ymd) {
	print "Content-type: text/html\n\n";
	#print "Access-Control-Allow-Origin: *";
	print "<html>";
	print "<header><title>Usage FAIL</title></header>";
	print "<body>";
	print "Start date must be before end date \n";
	print "</body>";
	print "</html>";
	exit;
    }

    if ($start_ymd == $end_ymd) {
	if ($start_hh > $end_hh) {
	    print "Content-type: text/html\n\n";
	    #print "Access-Control-Allow-Origin: *";
	    print "<html>";
	    print "<header><title>Usage FAIL</title></header>";
	    print "<body>";
	    print "Start date must be before end date \n";
	    print "</body>";
	    print "</html>";
	    exit;
	}
	if ($start_hh == $end_hh) {
	    if ($start_mm > $end_mm) {
		print "Content-type: text/html\n\n";
		#print "Access-Control-Allow-Origin: *";
		print "<html>";
		print "<header><title>Usage FAIL</title></header>";
		print "<body>";
		print "Start date must be before end date \n";
		print "</body>";
		print "</html>";
		exit;
	    }
	    if ($start_mm == $end_mm) {
		print "Content-type: text/html\n\n";
		#print "Access-Control-Allow-Origin: *";
		print "<html>";
		print "<header><title>Usage FAIL</title></header>";
		print "<body>";
		print "Start date must be before end date \n";
		print "</body>";
		print "</html>";
		exit;
	    }
	}
    }
    else {
	if ($end_ymd != $start_ymd) {
	    if ($start_mo == $end_mo) {
		if (($end_ymd < $start_ymd) > 1){
		    print "Content-type: text/html\n\n";
		    #print "Access-Control-Allow-Origin: *";
		    print "<html>";
		    print "<header><title>Usage FAIL</title></header>";
		    print "<body>";
		    print "Requested dataset too long\n";
		    print "</body>";
		    print "</html>";
		    exit;
		}
	    }
	    else {
		if ($start_yyyy != $end_yyyy) {
		    print "Content-type: text/html\n\n";
		    #print "Access-Control-Allow-Origin: *";
		    print "<html>";
		    print "<header><title>Usage FAIL</title></header>";
		    print "<body>";
		    print "Requested dataset too long\n";
		    print "</body>";
		    print "</html>";
		    exit;
		}
		else {
		    if (($end_mo - $start_mo) > 1) {
			print "Content-type: text/html\n\n";
			#print "Access-Control-Allow-Origin: *";
			print "<html>";
			print "<header><title>Usage FAIL</title></header>";
			print "<body>";
			print "Requested dataset too long\n";
			print "</body>";
			print "</html>";
			exit;
		    }
		    else {
			my $tmpdd = $end_dd;
			$tmpdd =~ s/^0//;
			if (($tmpdd != 1) || ($start_dd < 28)) {
			    print "Content-type: text/html\n\n";
			    #print "Access-Control-Allow-Origin: *";
			    print "<html>";
			    print "<header><title>Usage FAIL</title></header>";
			    print "<body>";
			    print "Requested dataset too long\n";
			    print "</body>";
			    print "</html>";
			    exit;
			}
		    }
		}
	    }
	}
    }
    if (!-d "$DATA_LOCATION/$start_ymd") {
	print "Content-type: text/html\n\n";
	#print "Access-Control-Allow-Origin: *";
	print "<html>";
	print "<header><title>Usage FAIL</title></header>";
	print "<body>";
	print "Sorry, data not available\n";
	print "</body>";
	print "</html>";
	exit;
    }
}
