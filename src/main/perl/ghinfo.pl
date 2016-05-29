#!/usr/bin/perl
use strict;
use warnings;

use Pithub;
use Data::Dumper;
use File::Slurp;

# Find all of the commentors on a PR
my $user = $ENV{'user'};
my $repo = $ENV{'repo'};
my $p = Pithub->new(user => $user, repo => $repo);
while (my $id = <>) {
    chomp ($id);
    my $pr_comments(p->pull_requests->comments->list(pull_request_id => $id));
    while (my $comment = $issue_comments->next) {
	print $comment->{"user"}->{"login"}."\n";
    }
}
