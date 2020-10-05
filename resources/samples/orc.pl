#!/usr/bin/perl

use strict;
use warnings FATAL => 'all';
while (<>) {
    if (/^\d+/) {
        my $id = $&;
        my $new_id = 1000 + $id;
        s/^$id/$new_id/;
        print;
    }
}
