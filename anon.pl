#!/usr/bin/perl -CS

use strict;
use warnings;
use utf8;

use Data::Dumper;
use Memoize;
use Text::CSV;

my $parser = Text::CSV->new( { binary => 1, quote_char => "'", escape_char => '\\', keep_meta_info => 1, allow_loose_escapes => 1, always_quote => 1 });

# locatin of .csv filed for config anon data
my $source_dir = '.';

my %seen;
my $create_table_name;
my $column_number = 0;
my $column_name;
my $inside_create = 0;
my $insert_table_name;
my $inside_insert = 0;
my %table;
my %table_reverse;
my %column;
my %column_reverse;
my %contains_anon_column;
my %anon_columns;
my %anon_data;
my %data_types;
my %quoted_types = map { $_ => 1 } qw( bit char datetime longtext text varchar );

memoize('get_anon_col_index');

# load fake data
load_config();
load_anon_data();


binmode STDOUT, ":utf8";
#binmode STDIN, ":utf8";

while(<>){

  if ($inside_create == 1 && $_ =~ /ENGINE=(InnoDB|MyISAM)/) {
    $inside_create = 0; # create statement is finished
  }

  if ($inside_create == 0 && $_ =~ /^CREATE TABLE `([a-z0-9_]+)` \(/) {
    create_table($1);
  }

  if ($inside_create == 1 && $_ =~ /`([A-z0-9_]+)`\s([a-z]+)/ && $_ !~ /CREATE TABLE/) {
    inside_create($1, $2); # parse create statement to index column positions
  }

  if($_ =~ /^(INSERT INTO `([a-z0-9_]+)` VALUES\s\()/) {
    inside_insert($1, $2); # anonymize VALUES statement
  }
  else {
    # this line won't be modified so just print it.
    print
  }

  if($inside_insert == 1 && /\);\n/) {
    $inside_insert = 0; # This insert is finished
  }

}


sub get_value {
  # Get a value from the array.  Array is looped so we don't run out of values
  my $type = shift;
  my $value;
  if($type eq 'random') {
    $value = random_string();
  }
  else {
    $value = shift @{$anon_data{$type}};
    push @{$anon_data{$type}}, $value;
  }

  return $value;
}


sub create_table {
  my $table = shift;
  $create_table_name = $table; #Store current table name
  $column_number = 0; # new create statement, reset column count
  $inside_create = 1;
}

sub inside_create {
  # process create statment to record ordinal position of columns
  my ($column, $type) = @_;
  $column_name = $column;
  if(exists $anon_columns{$create_table_name}{$column_name}) {
    $table{$create_table_name}{$column_name} = $column_number;
    $table_reverse{$create_table_name}{$column_number} = $column_name;
  }
  $data_types{$create_table_name}{$column_number} = $type;
  $column_number++;
}


sub inside_insert {
  my ($a1, $a2) = @_;
  $insert_table_name = $a2;
  my $start_of_string = $a1;
  $inside_insert = 1;

  if(exists $table{$insert_table_name}) { # table contains anon candidate
    # split insert statement
    my @lines = split('\),\(', $_);
    $lines[0] =~ s/\Q$start_of_string\E//g; # remove start of insert string, only interested in the "values"
    $lines[$#lines] =~ s/\);\n//g; # remove trailing bracket from last line of insert

    # loop through each line
    foreach my $line (0..$#lines) {

      # use Text::CSV to parse the values
      my $status = $parser->parse($lines[$line]);
      my @columns = $parser->fields(); if($#columns == 0) { print $lines[$line], "\n"; die "\noops\n", $parser->error_input(); exit }

      # store quote status foreach column
      #my @quoted;
      #foreach my $index (0..$#columns) {
      #  push @quoted, $parser->is_quoted ($index);
      #}

      # replace selected columns with anon value
      map {
        my $new_val = $anon_columns{ $insert_table_name }{ $table_reverse{$insert_table_name }{$_} }{ 'dispatch' }->();
        # make sure new value is no longer than old value
        my $length_old = length($columns[$_]);
        if ($columns[$_] ne 'NULL' ) {
          # only anonymize if not null
          $columns[$_] = substr($new_val, 0, $length_old)
        }
      } get_anon_col_index($insert_table_name);

      # put quotes back
      foreach my $index (0..$#columns) {
  die " $insert_table_name $index " , Dumper(%data_types) if ! exists $data_types{$insert_table_name}{$index};
        if (exists $quoted_types{$data_types{$insert_table_name}{$index}} && $columns[$index] ne 'NULL') {

          # binary 1 & 0 mangled by Text::CSV, replace with unquoted 1 & 0
          my $bin_1 = quotemeta(chr(1));
          my $bin_0 = quotemeta(chr(0));
          if($columns[$index] =~ /$bin_1/) {
            $columns[$index] = 1; # if binary 1, set unquoted integer 1
          }
          elsif ($columns[$index] =~ /$bin_0/) {
             $columns[$index] = 0; # if binary 0, set unquoted integer 0
          }
          else {
            # use Text:CSV to add quotes - it will escape any quotes in the string
            $parser->combine( $columns[$index] );
            $columns[$index] =  $parser->string;
          }
        }
      }
      die "qrtz?", Dumper(%anon_columns) if $create_table_name eq 'qrtz_job_details';
      # put the columns back together
      $lines[$line] = join(',', @columns);
    }
    # reconstunct entire insert statement and print out
    print $start_of_string . join('),(', @lines) . ");\n";
  }
  else {
    print # print unmodifed insert
  }

}

sub get_anon_col_index {
  # returns an array of column ordinal postions for columns that are marked for anonymization
  my $table_name = shift;
  my @idx;
  foreach my $col (keys %{ $table{ $table_name } } ) {
    if (exists $table{$table_name}{$col}) {
      push @idx, $table{$table_name}{$col};
    }
  }
  return sort @idx;

}

sub load_anon_data {
  # load in the anon data
  my @filenames = qw{ fakeAddr1.csv fakeCompanyName.csv fakeContactName.csv fakeEmail.csv fakePhone.csv fakeURL.csv };
  my @types = qw{ address companyname contactname email phone url };

  foreach my $file (@filenames) {
    open(my $fh, "<", $source_dir . '/' . $file);
    my $type = shift @types;
    my @data = undef;
    while(<$fh>) {
      chomp;
      tr/"//d;
      tr/'//d;
      push @data, $_;
      last if $. == 5000; # only load the first 5000 entries
    }
    shift @data;
    $anon_data{$type} = \@data;
  }
}

sub load_config {

  # valid types
  my %types = map { $_ => 1 } qw( address companyname contactname email phone url random );

  open(my $fh, "<", $source_dir . '/' . "config.csv");
  while(<$fh>) {
    chomp;
    s/\s+//g;
    my ($tbl_name, $col_name, $type) = split/,/;
      die "Invalid type in config.csv:\n$_" if ! exists $types{$type};
      $anon_columns{ $tbl_name }{ $col_name }{'dispatch'} = sub { get_value($type) };
  }
  #print Dumper(%anon_columns);
}

sub random_string {
  my @chars = ("A".."Z", "a".."z");
  my $string;
  while(1) {
    $string .= $chars[ rand @chars ] for 1..8;
     last if ! $seen{$string};
  }
  return $string;
}
