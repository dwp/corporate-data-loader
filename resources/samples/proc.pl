#!/usr/bin/perl

use strict;
use warnings;

while (<>) {
    chomp;
    my @data = split(',');
    print join(",", @data[0..6]), "\n";
}
