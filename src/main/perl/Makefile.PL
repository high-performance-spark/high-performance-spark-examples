use 5.006;
use strict;
use warnings;
use ExtUtils::MakeMaker;

WriteMakefile(
    NAME             => 'HighPerformanceSpark::Examples',
    AUTHOR           => q{Holden Karau And Rachel Warren <high-performance-spark@googlegroups.com>},
    VERSION_FROM     => 'lib/HighPerformanceSpark/Examples.pm',
    ABSTRACT_FROM    => 'lib/HighPerformanceSpark/Examples.pm',
    LICENSE          => 'apache_2_0',
    PL_FILES         => {},
    EXE_FILES        => [ 'ghinfo.pl' ],
    MIN_PERL_VERSION => 5.006,
    CONFIGURE_REQUIRES => {
        'ExtUtils::MakeMaker' => 0,
    },
    BUILD_REQUIRES => {
        'Test::More' => 0,
    },
    PREREQ_PM => {
        'Pithub'            => 0.01033,
        #'ABC'              => 1.6,
        #'Foo::Bar::Module' => 5.0401,
    },
    dist  => { COMPRESS => 'gzip -9f', SUFFIX => 'gz', },
    clean => { FILES => 'HighPerformanceSpark-Examples-*' },
);
