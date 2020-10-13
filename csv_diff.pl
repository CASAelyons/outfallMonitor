use strict;
use File::Copy;
my @arr1;
my @arr2;
our $input_csv1;
our $input_csv2;
our $output_diff_csv;
my $a;

sub command_line_parse {
    if (($#ARGV < 0) || ($#ARGV > 2)) {
	print "Usage:  csv_diff.pl csv_file1 csv_file2 csv_diff_file\n";
	exit;
    }
    $input_csv1 = $ARGV[0];
    $input_csv2 = $ARGV[1];
    $output_diff_csv = $ARGV[2];
}

&command_line_parse;

open(FIL,$input_csv1) or die("$!");
while (<FIL>)
{chomp; $a=$_; $a =~ s/[\t;]*//g; push @arr1, $a if ($a ne  '');};
close(FIL);

open(FIL,$input_csv2) or die("$!");
while (<FIL>)
{chomp; $a=$_; $a =~ s/[\t;]*//g; push @arr2, $a if ($a ne  '');};
close(FIL);

my %arr1hash;
my %arr2hash;
my @diffarr;
foreach(@arr1) {$arr1hash{$_} = 1; }
foreach(@arr2) {$arr2hash{$_} = 1; }

foreach $a(@arr1)
{
    if (not defined($arr2hash{$a}))
    {
	push @diffarr, $a;
    }
}

foreach $a(@arr2)
{
    if (not defined($arr1hash{$a}))
    {
	push @diffarr, $a;
    }
}

if (!@diffarr) {
    exit;
}
else {
    open(FIL, '>', $output_diff_csv) or die("$!");
    foreach $a(@diffarr) {
	print FIL "$a\n";
    }
    close(FIL);
    copy($input_csv1, $input_csv2);
}

