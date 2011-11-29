package DBTools;

use strict;
use warnings;
use Carp; use Carp::Heavy;
use feature ":5.10";


# Author: Ingo Lantschner (ingo[at]lantschner.name)

use English '-no_match_vars';
use Data::Dumper;
use Readonly;
use DBI;
use SQL::Abstract;
use DateTime;

our $VERSION = '1.2.4';

use DBToolsCfg qw( :credentials $LOG_FILE);

use base qw(Exporter);
our @EXPORT_OK = qw( 
         $BLANK
         $COLON
         $COMMA
         $CRLF
         $DASH
         $DEFAULT_LABEL
         $DOT
         $D_QUOT
         $FALSE
         $MISSING_VALUE
         $S_QUOT
         $EMPTY
         $SLASH
         $TRUE
         $UNDERSCORE
         debug
         i
);
our %EXPORT_TAGS = (
    special_chars   => [ qw(
        $BLANK     
        $CRLF
        $EMPTY
        $DASH     
        $DOT       
        $COMMA     
        $COLON     
        $SLASH     
        $D_QUOT    
        $S_QUOT    
        $UNDERSCORE
    )],
    ab_defaults           => [ qw(
        $DEFAULT_LABEL
        $MISSING_VALUE
        $TRUE
        $FALSE
    )], 
    debug           => [ qw(
        debug
        i
    )], 
);

# Globale Variablen und Konstante
Readonly our $BLANK     => q{ };
Readonly our $EMPTY     => q{};
Readonly our $DOT       => q{.};
Readonly our $COMMA     => q{,};
Readonly our $COLON     => q{:};
Readonly our $DASH      => q{-};
Readonly our $SLASH     => q{/};
Readonly our $D_QUOT    => q{"};
Readonly our $S_QUOT    => q{'};
Readonly our $UNDERSCORE => q{_};
Readonly our $CRLF      => q{\x0d\x0a};
Readonly our $MISSING_VALUE => q{missing value}; # returnd from AB for non-existing fields
Readonly our $DEFAULT_LABEL => q{other}; # Default-label for AB-properties like phone and email
Readonly our $TRUE => q{true};   # for boolean properties in AB
Readonly our $FALSE => q{false}; # for boolean properties in AB

# exported subs
sub i {
    my $msg = shift;
    my ($package, $filename, $line) = caller;
    my $sep = $BLANK;
    print {*STDERR} $package . $sep . $line . $sep . $msg . "\n" 
        if $main::DEBUG > 1;
    return;
}

sub debug {
    my $level = shift;
    my $msg = shift;
    my ($package, $filename, $line) = caller;
    my $sep = $BLANK;
    my $logg = 0;
    $logg = $main::LOGG  if defined $main::LOGG;
    
    if ( $main::DEBUG > ($level - 1) ){
        print {*STDERR} "$package" . "$sep" . "$line" . "$sep" . "$msg" .  "\n";
    }   # STDOUT hier stört die Rückgabewerte für das Apple-Script!!
    if ( $logg > ($level - 1) ){
        open my $fh, '>>', "$LOG_FILE"
            or die "Error opening $LOG_FILE: $!";
        print $fh time() . ": ";
        print $fh  "$package" . "$sep" . "$line" . "$sep" . "$msg" .  "\n";
        close $fh
            or die "Error closing $LOG_FILE: $!";
    }
    return;
}

# ===========
# = OO-part =
# ===========

=head1 DBTools

Tools for syncing Apples Addressbook with an RDBMS. 
Also used for the BA-database (Buchhaltung, Belege)

Methods in singular are meant to affect/return a single row where the colums data 
equals a value. Therefore colums with non-unique values are seldom useable for 
comparison.

Methods in plural receive more complex where-keys (as usable for SQL::Abstract) 
and may affect/return more than one row.

=head2 SYNOPSIS

If a databasehandle is already in place:
    
    our $DEBUG = 0;
    our LOGG  = 1;
    my $db = DBTools->new(dbh => $dbh);

    $db->insert_row("$table_name", \%data);
    my $id = $db->get_id(val => 'Big Corp Ltd.');
    my $vat_id = $db->get_id(col => 'name', val => 'Big Corp Ltd.', col_to_return = 'uid');
    $db->delete_row(table => 'organisations', val => 9999909);

If the database should be connected:

    my $db = DBTools->new(db_name => 'BA', db_pass => 'mypass');

=head3 CONFIGURATION

A small perl-module holds user- /site-specific information. Please create it
in the same directory like this module. 

Example-Code and Tenmplate:

    package DBToolsCfg;
    use warnings;
    use strict;
    use Readonly;
    use base qw(Exporter);
    our @EXPORT_OK = qw( 
        $DEFAULT_USER
        $DEFAULT_PASS
        $LOG_FILE
    );
    our %EXPORT_TAGS = (
        credentials   => [ qw(
            $DEFAULT_USER
            $DEFAULT_PASS
        )],
    );

    # Globale Variablen und Konstante
    Readonly our $DEFAULT_USER     => q{yourname};
    Readonly our $DEFAULT_PASS     => q{yourpass};
    Readonly our $LOG_FILE => q{/your/path/to/logs/DBTools.log}; 

=cut

sub new{
    my ($class, %arg) = @_;
    my $self = bless{
        _dbh    => $arg{'dbh'},
        _debug  => $main::DEBUG // 0,
        _quote_char => $arg{'quote_char'} // "$EMPTY",  # q{`}
    }, $class;
    croak 'no $DEBUG in main' if not defined $main::DEBUG;
    if (not defined $arg{'dbh'}) {
        $self->{'_dbh'} = $self->_init(\%arg);
    }
    return $self;
}

sub _init {
    my ($self, $arg) = @_;
    
    # Initalize the DB connection
    my $db_name = ${$arg}{'db_name'} // croak('missing argument db_name');
    my $db_host = ${$arg}{'db_host'} // 'localhost';
    my $db_user = ${$arg}{'db_user'} // "$DEFAULT_USER";
    my $db_pass = ${$arg}{'db_pass'} // "$DEFAULT_PASS";
    my $db_driver = ${$arg}{'db_driver'} // 'mysql';

    my $dbh = DBI->connect("DBI:$db_driver:$db_name", "$db_user", "$db_pass") 
        || croak "Could not connect to database: $DBI::errstr";
    $dbh->do(qq{SET NAMES 'utf8';}); # German Umlauts et al (unicode)
    return $dbh;
}

=head2 METHODS

=cut

sub insert_row {

=head2 insert_row

Receives a hash, where the column-names are the keys and the values are the fields.
Inserts this hash into the table of the database.

The tablename is the first arg, the hash-ref the second.

The third, optional argument defaults to 'id' and defines the column value
which gets returned to the caller.


=head3 EXAMPLES


Using a prviously cerated hash:

    $db->insert_row('organisations', \%data);
    

Using an anonymous hash: This will insert one row with two fields into the 
table p_groups
    
    my $group_id = $db->insert_row(
        'p_groups', 
        {
            p_id => $id,
            group_name => $group_name
        },
    );
    

=cut
    my ($self, @argv) = @_;
    my ($table, $data) = @argv;
    debug(2, 'table name: ' . $table);
    debug(3, 'Dump of $data: ' . Dumper($data));
    my $dbh = $self->{'_dbh'};
    my $sql = SQL::Abstract->new(quote_char => qq{$self->{'_quote_char'}});
    
    # update 'created'
    if ($self->has_column(table => $table, col => 'created') ) {
        if (exists $data->{'created'}) {
            warn q{Key 'created' already exists in data - wont change it!};
        } else {
            $data->{'created'} = (DateTime->now)->iso8601;
        }
    }
    
    my($stmt, @bind) = $sql->insert($table, $data);
    debug(2, "\$stmt: $stmt");
    #debug(2, "\@bind: @bind");  # sonst meckert er wegen undef-Werten
    my $sth = $dbh->prepare($stmt);
    my $r = $sth->execute(@bind) or die $sth->errstr;
    my $id = $dbh->last_insert_id(undef, undef, $table, $self->{'_id'});
    return $id;
}

sub has_column {
    my ($self, @argv) = @_;
    my %arg = (
        table => undef,
        col => 'created',
        @argv,
    );
    debug(2, '\%arg: ' . Dumper(\%arg));
    croak q{missing argument 'table'} if not defined $arg{'table'};
    my $dbh = $self->{'_dbh'};
    my $sth = $dbh->column_info( undef, undef, $arg{'table'}, undef );
    my $info = $sth->fetchall_hashref('COLUMN_NAME');
    debug(2, q{Dump of '$info'} . "\n" . Dumper($info));
    if (exists $info->{$arg{'col'}}) {
        debug(1, "Column $arg{'col'} exists in table $arg{'table'}");
        return 1;
    } 
    return;
}

sub update_rows {
=head2 update_rows

Updates all rows matching the where-statemnt. Number can be limited by the
'max'-argument, which defaults to 1. If more than max rows would be affected,
a warning is send and nothing is changed in the database.

Returns the number of changed rows.

=head3 EXAMPLES

    # Prepare a hash where the keys matches the col-names in the DB. 
    my %data; 
    
    # Do some regex-maching and copy the results to the hash-keys
    $data{'country_code'}   = $1;
    $data{'area_code'}      = $2;
    $data{'number'}         = $3;
    $data{'ext'}            = $4;
    
    # update some of the rows values ... (max defaults to 1, so only 1 row is changed)
    $db->update_rows(
        table => 'p_tel',
        where => {id => $id},
        data => \%data,
    );


=cut
    my ($self, @argv) = @_;
    my %arg = (
        table => undef,
        where => '',
        data => '',
        max => 1,
        dry_run => 0,
        @argv,
    );
    debug(2, '\%arg: ' . Dumper(\%arg));
    croak q{missing argument 'table'} if not defined $arg{'table'};
    debug(2, 'where: ' . Dumper($arg{'where'}));
    debug(2, 'data: ' . Dumper($arg{'data'}));
    
    my $dbh = $self->{'_dbh'};
    $self->_check_max($arg{'table'}, $arg{'where'}, $arg{'max'}) or return;
    if (not $arg{'dry_run'}) {
        my $sql = SQL::Abstract->new(quote_char => qq{$self->{'_quote_char'}});
        my ($stmt, @bind) = $sql->update($arg{'table'}, $arg{'data'}, $arg{'where'});
        my $sth = $dbh->prepare($stmt);
        my $rows_affected = $sth->execute(@bind) or die $sth->errstr;
        return $rows_affected;
    } else {
        my $r = $self->get_ids(table => $arg{'table'}, where => $arg{'where'});
        return @{$r};
    }
}

sub get_id {
=head2 get_id

Returns the id (or any other columes value)  of a single row if the columns 
value equals the val.

TODO: What if more than one row matches?

=cut
    my ($self, @argv) = @_;
    my %arg = (
        'table' => 'organisations',
        'col' => 'name', 
        'val' => undef,
        'col_to_return' => 'id',
        @argv,
    );
    debug(2, '\%arg: ' . Dumper(\%arg));
    my $dbh = $self->{'_dbh'};
    my $row = $dbh->selectrow_hashref ("SELECT * FROM " . $arg{'table'} . " WHERE " . $arg{'col'} . "=?", undef, $arg{'val'} );
    debug(2, "Dump of row:\n" . Dumper($row));
    return $row->{ $arg{'col_to_return'} };
}

sub get_ids {
=head2 get_ids

Returns the ids (or any other columes value)  of all rows matching the where statement.
An empty or undef where-argument returns all.

=head3 Example

    my @ids = $db->get_ids(table => 'belege', where => 'beleg_datum IS NULL');

=cut
    my ($self, @argv) = @_;
    my %arg = (
        table => undef,
        where => '',
        order => '',
        col_to_return => 'id',
        @argv,
    );
    debug(2, '\%arg: ' . Dumper(\%arg));
    croak q{missing argument 'table'} if not defined $arg{'table'};
    my $dbh = $self->{'_dbh'};
    my $sql = SQL::Abstract->new(quote_char => qq{$self->{'_quote_char'}});
    my($stmt, @bind) = $sql->select($arg{'table'}, 
                        $arg{'col_to_return'}, 
                        $arg{'where'}, 
                        $arg{'order'}
                      );
    my $sth = $dbh->prepare($stmt);
    my $rv = $sth->execute(@bind);
    my $r = $sth->fetchall_arrayref or die $sth->errstr;
    debug(2, "Dump of r:\n" . Dumper($r));
    my @ids;
    foreach (@{$r}) {
        push @ids, ${$_}[0]; 
    }
    return \@ids;
}

sub get_row {
=head2 get_row

Receives one row as a hashref if the value of val matches the column id (deafault).

    my $beleg = $db->get_row(table => 'belege', val => $beleg_id);
        if (${$beleg}{'bezahldatum'} =~ $this_month) { ...}

=cut
    my ($self, @argv) = @_;
    my %arg = (
        col => 'id',
        val => undef,
        table => undef,
        @argv,
    );
    debug(2, '\%arg: ' . Dumper(\%arg));
    croak q{missing argument 'table'} if not defined $arg{'table'};
    croak q{missing argument 'val'} if not defined $arg{'val'};
    my $dbh = $self->{'_dbh'};
    my $row = $dbh->selectrow_hashref (
        "SELECT * FROM " . $arg{'table'} . " WHERE " . $arg{'col'} . "=?", 
        undef, $arg{'val'} 
    ); # end of $row =
    return $row;
}

sub get_rows {
=head2 get_rows

Receives several rows as a hashref.

    my $bh_list = $db->get_rows(table => 'buchhaltungen', key_field => 'kurz_name');
    
If complex statments are nedded, use sql => 

    my $zahlungen = $db->get_rows( 
        sql => {
            stmt => q{  SELECT apartment, r.subkto, SUM(`betrag`) FROM `zahlungen` z JOIN rechnungen r 
                ON z.`renr` = r.`renr`
                WHERE z.datum > ?
                GROUP BY `apartment`,subkto},
            bind => [ '2011-10-01' ],

        },
        key_field => ['apartment', 'subkto'] 
    ); # end of $db->get_rows
    

=cut
    my ($self, @argv) = @_;
    my %arg = (
        key_field => 'id',
        where => '',
        table => undef,
        col_to_return => '*',
        order => undef,     ## # default set to 'key_field' below!
        sql =>  undef,
        @argv,
    );
    debug(2, '\%arg: ' . Dumper(\%arg));
    my $dbh = $self->{'_dbh'};
    my $sql = SQL::Abstract->new(quote_char => qq{$self->{'_quote_char'}});
    my($stmt, $bind);
    if (defined $arg{'sql'}) {
        $stmt = ${$arg{'sql'}}{'stmt'};
        $bind = @{$arg{'sql'}}{'bind'};
    } else {
        croak q{missing argument 'table'} if not defined $arg{'table'};
        ($stmt, @$bind) = $sql->select($arg{'table'}, 
                            $arg{'col_to_return'}, 
                            $arg{'where'}, 
                            $arg{'order'}
                          );
    }
    debug(2, q{Dump of '@$bind'} . "\n" . Dumper(@$bind));
    my $rows = $dbh->selectall_hashref($stmt, $arg{'key_field'}, undef, @$bind)
        or croak 'error in get_rows';
    return $rows;
}

sub delete_row {
=head2 delete_row
    
    Deltes all rows (plural) where col = val. This is an exeption from the rule,
    that singular-named methods affect only one role.
    
    The argument max defaulting to one limits the number of rows deleted.
    
=head3 Example

    $db->delete_row(
        table => 'girokonto',
        col => 'Betrag',
        val => 0,
        dry_run => $DRY_RUN,
        max => 1,
    );

=cut
    my ($self, @argv) = @_;
    my %arg = (
        col => 'id',
        val => undef,
        table => 'organisations',
        max => 1,
        @argv,
    );
    debug(1, '\%arg: ' . Dumper(\%arg));
    $self->_check_max($arg{'table'}, {$arg{'col'} => $arg{'val'}}, $arg{'max'}) or return;
    my $dbh = $self->{'_dbh'};
    my $statement = qq{DELETE FROM $arg{'table'} WHERE $arg{'col'}=?};
    my $r = $dbh->do($statement, undef, $arg{'val'}) 
        // die 'undefined return-value from $dbh->do';
    return $r;
}

=head1 NON-PUBLIC METHODS
=cut

sub _check_max {
=head2 _check_max

Returns 1 if the number of rows matching the where-statement is not greater than
the max-value. Else warns and returns undef.

=cut
    my ($self, $table, $where, $max) = @_;
    my $dbh = $self->{'_dbh'};
    my $sql = SQL::Abstract->new(quote_char => qq{$self->{'_quote_char'}});
    my($stmt, @bind) = $sql->select($table, '*', $where);
    my $sth_check = $dbh->prepare($stmt);
    my $rv = $sth_check->execute(@bind);
    if ($rv > $max) {
        warn qq/Too many rows ($rv) affected! You may want to change max ?!/;
        return;
    } else {
        return 1;
    };
    return;
}


1;
__END__