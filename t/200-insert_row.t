#!/usr/local/bin/perl -w
use warnings;
use strict;
use Carp; use Carp::Heavy;
use feature ":5.10";

use Test::More 'no_plan'; #tests => 13;
use Test::Warn;
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
my $table  = 'organisations';  # will be changed later in this script!

can_ok('DBTools', 'new');
my $db = DBTools->new(db_name => $db_name, db_user  => $db_user, db_pass => $db_pass);

can_ok('DBTools', 'insert_row');
my %data = (
    id => 999999200,
    name => 'unit-test-200-01',
    uid => 'UT001',
    cc => 'ut01',
);

# ============
# = Standard =
# ============
is ($db->insert_row("$table", \%data), $data{'id'}, "insert returns id" );
is ($db->get_id(table => $table, col_to_return => 'created', val => $data{'id'} ), undef, q{no creation timestamp if table has no 'created' column});
is ($db->delete_row(table => "$table", val => $data{'id'}), 1, "deleting one row returns 1" );

# ==========================================
# = Tabelle mit passender Spalte 'created' =
# ==========================================
$table = 'organisations_with_created';
my $ymd = (DateTime->now)->ymd;
is ($db->insert_row("$table", \%data), $data{'id'}, "insert returns id" );
like (
    $db->get_id(table => $table, col_to_return => 'created', col => 'id', val => $data{'id'} ), 
    qr/$ymd/, 
    q{creation timestamp matches if table has 'created' column})
;  ## end of like
is ($db->delete_row(table => "$table", val => $data{'id'}), 1, "deleting one row returns 1" );

# =============================
# = Wert für created mitgeben =
# =============================
$table = 'organisations_with_created';
$ymd = "2011-11-01";
%data = (
    id => 999999200,
    name => 'unit-test-200-03',
    uid => 'UT001',
    cc => 'ut01',
    created => "$ymd",
);

warning_like {$db->insert_row("$table", \%data), $data{'id'}}
    qr/Key 'created' already exists in data/,
    q{warns if 'created' is in data} 
; # end of warning_like

like (
    $db->get_id(table => $table, col_to_return => 'created', col => 'id', val => $data{'id'} ), 
    qr/$ymd/, 
    q{creation timestamp matches time-stamp from data})
;  ## end of like
is ($db->delete_row(table => "$table", val => $data{'id'}), 1, "deleting one row returns 1" );

__END__


