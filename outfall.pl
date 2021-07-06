#!/usr/bin/perl -w
use strict;
use warnings;
use CGI;
use DBI;

# output the content-type so the web server knows

my $cgi = CGI->new;

print $cgi->header();
$cgi->start_html("DFW Airport Outfall Status");

my $db_host = 'localhost';
my $db_user = 'dfw';
my $db_pass = 'outfall';
my $db_name = 'dfwairport';
my $db = "dbi:Pg:dbname=${db_name};host=${db_host}";
my $dbh = DBI->connect($db, $db_user, $db_pass, { RaiseError => 1 })
    or die $DBI::errstr;

my $sql = q/select * from outfalls/;
my $sth = $dbh->prepare($sql);
$sth->execute;

#print '<table border="1">';

#print "<tr><th>$sth->{NAME}->[0]</th><th>$sth->{NAME}->[1]</th><th>$sth->{NAME}->[2]</th><th>$sth->{NAME}->[3]</th><th>$sth->{NAME}->[4]</th><th>$sth->{NAME}->[5]</th><th>$sth->{NAME}->[6]</th><th>$sth->{NAME}->[7]</th></tr>";

print $cgi->table({-border=>1}), $cgi->Tr ( $cgi->th( $sth->{NAME}->[0]) . $cgi->th($sth->{NAME}->[7]) . $cgi->th($sth->{NAME}->[2]) . $cgi->th($sth->{NAME}->[1]) . $cgi->th($sth->{NAME}->[3]) . $cgi->th($sth->{NAME}->[4]) . $cgi->th($sth->{NAME}->[5]) . $cgi->th($sth->{NAME}->[6]));
		  
while (my @row = $sth->fetchrow_array) {
    my $prodtype;
    if ($row[2] == 1) {
	$prodtype = "streamflow";
    }
    elsif ($row[2] == 2) {
	$prodtype = "velocity";
    }
    elsif ($row[2] == 4) {
	$prodtype = "water level";
    }
    else {
	$prodtype = "streamflow";
    }
    
    my $abvbaseflow_img;
    if (( $row[3] // '' ) eq '' ) {
	$abvbaseflow_img = $cgi->center($cgi->img({ -src => "https://emmy10.casa.umass.edu/images/off.png" }));
    }
    elsif ($row[3] == 0) {
	$abvbaseflow_img = $cgi->center($cgi->img({ -src => "https://emmy10.casa.umass.edu/images/off.png" }));
    }
    else {
	$abvbaseflow_img = $cgi->center($cgi->img({ -src => "https://emmy10.casa.umass.edu/images/on.png" }));
    }
    my $waiting_img;
    if (( $row[4] // '' ) eq '' ) {
	$waiting_img = $cgi->center($cgi->img({ -src => "https://emmy10.casa.umass.edu/images/off.png" }));
    }
    elsif ($row[4] == 0) {
	$waiting_img = $cgi->center($cgi->img({ -src => "https://emmy10.casa.umass.edu/images/off.png" }));
    }
    else {
	$waiting_img = $cgi->center($cgi->img({ -src => "https://emmy10.casa.umass.edu/images/on.png" }));
    }
		
    my $accumulating_img;
    if (( $row[5] // '' ) eq '' ) {
	$accumulating_img = $cgi->center($cgi->img({ -src => "https://emmy10.casa.umass.edu/images/off.png" }));
    }
    elsif ($row[5] == 0) {
	$accumulating_img = $cgi->center($cgi->img({ -src => "https://emmy10.casa.umass.edu/images/off.png" }));
    }
    else {
	$accumulating_img = $cgi->center($cgi->img({ -src => "https://emmy10.casa.umass.edu/images/on.png" }));
    }
    
    print $cgi->Tr( $cgi->td( $row[0] ) . $cgi->td( $cgi->center($row[7] )) . $cgi->td( $prodtype ) . $cgi->td( $cgi->center($row[1] )) . $cgi->td ( $abvbaseflow_img ) . $cgi->td ( $waiting_img ) . $cgi->td ( $accumulating_img ) . $cgi->td( $cgi->center($row[6]) ));
};

$sth->finish;

$sql = q/select * from events/;
$sth = $dbh->prepare($sql);
$sth->execute;

print $cgi->table({-border=>1}), $cgi->Tr ( $cgi->th( $sth->{NAME}->[0]) . $cgi->th($sth->{NAME}->[1]) . $cgi->th($sth->{NAME}->[2]) . $cgi->th($sth->{NAME}->[3]) . $cgi->th($sth->{NAME}->[4]) . $cgi->th($sth->{NAME}->[5]) . $cgi->th($sth->{NAME}->[6]) . $cgi->th($sth->{NAME}->[7]) . $cgi->th($sth->{NAME}->[8]) . $cgi->th($sth->{NAME}->[9]) . $cgi->th($sth->{NAME}->[10]));

while (my @row = $sth->fetchrow_array) {
    my $prodtype;
    if ($row[8] == 1) {
	$prodtype = "streamflow";
    }
    elsif ($row[8] == 2) {
	$prodtype = "velocity";
    }
    elsif ($row[8] == 4) {
	$prodtype = "water level";
    }
    else {
	$prodtype = "streamflow";
    }
    
    print $cgi->Tr( $cgi->td( $cgi->center($row[0]) ) . $cgi->td( $cgi->center($row[1] )) . $cgi->td( $cgi->center($row[2] )) . $cgi->td ( $cgi->center($row[3])) . $cgi->td ( $cgi->center($row[4])) . $cgi->td ( $cgi->center($row[5])) . $cgi->td( $cgi->center($row[6])) . $cgi->td( $cgi->center($row[7])) . $cgi->td( $cgi->center($prodtype)) . $cgi->td( $cgi->center($row[9])) . $cgi->td( $cgi->center($row[10])));
}

$sth->finish;
$dbh->disconnect;

print "</body></html>\n";
