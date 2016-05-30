#!perl -T
use 5.006;
use strict;
use warnings;
use Test::More;

plan tests => 1;

BEGIN {
    use_ok( 'HighPerformanceSpark::Examples' ) || print "Bail out!\n";
}

diag( "Testing HighPerformanceSpark::Examples $HighPerformanceSpark::Examples::VERSION, Perl $], $^X" );
