#!/bin/bash
thisdate=`/bin/date -u +%Y%m%d-%H%M%S`
rclone copy dfw365:Shared\ Documents/023\ OUTPUT\ 10\ MIN.CSV /home/elyons/dfw; /usr/bin/perl /home/elyons/perl/csv_diff.pl /home/elyons/dfw/023\ OUTPUT\ 10\ MIN.CSV /home/elyons/dfw/outfall023.csv /home/elyons/dfwairport/23/Outfall023_$thisdate.csv; rclone copy dfw365:Shared\ Documents/020\ OUTPUT\ 5\ MIN.CSV /home/elyons/dfw; /usr/bin/perl /home/elyons/perl/csv_diff.pl /home/elyons/dfw/020\ OUTPUT\ 5\ MIN.CSV /home/elyons/dfw/outfall020.csv /home/elyons/dfwairport/20/Outfall020_$thisdate.csv;
