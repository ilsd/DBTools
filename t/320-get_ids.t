#!/usr/local/bin/perl -w
use warnings;
use strict;
use Carp; use Carp::Heavy;
use feature ":5.10";

use Test::More 'no_plan'; #tests => 13;
use Test::NoWarnings;
use Test::Exception;
use Readonly;

# Constants
our $DEBUG = 0;
our $LOGG = 0;

# my modules
use FindBin qw($Bin);
use lib "$Bin/../scripts";

use_ok('DBTools');

# Constants
Readonly my $db_name  => 'DBTools_unit-tests';
Readonly my $db_user  => 'unit_tests';
Readonly my $db_pass  => 'hello';
Readonly my $table  => 'organisations';

can_ok('DBTools', 'new');
my $db = DBTools->new(db_name => $db_name, db_user  => $db_user, db_pass => $db_pass);

can_ok('DBTools', 'get_ids');
throws_ok { $db->get_ids() } qr/missing argument 'table'/;
is(  @{$db->get_ids(table => 'belege')}, 18, '18 Einträge bei fehlendem where'  );

my %where = (
    id => 2,
);
is( @{$db->get_ids(table => 'belege', where => {id => 1})}, 1, 'returns one id' );


cmp_ok( 
    @{$db->get_ids(table => 'belege', where => {id => { '<>',  1}} )},
    '>', 0, 'returns more than one id' 
);


is( @{$db->get_ids(table => 'belege', where => {ablage_nr => 'E-Beleg'})}, 1, 
    'test mit Bindestrich' 
);

__END__