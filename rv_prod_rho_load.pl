#!C:/Perl64/bin/perl.exe

use strict;
use warnings;
use DBI;
use Cwd;
use CGI qw(:standard);
use Data::Dumper;

my $dsn = 'DBI:Oracle:server.domain.com:port/sname.domain.com';
my $database= 'xxxx';
my $user= 'xxxx';
my $pass = 'xxxx';

my $query = new CGI;

#declare variables:
#pick local directory
my $sql_location = cwd();
my $sql_table = $ARGV[0];
my $fic_mis_date = $ARGV[1]; #'31-MAR-2014'

    #connect to sql server and load data from xxxx prod
    my $dsn_rho = 'DBI:ODBC:Driver={SQL Server}';
    my $host_rho = 'xxxx\VEGA';
    my $database_rho = 'RHO';
    my $user_rho= '';
    my $auth_rho = '';
    
    #declare sub-elements		
    #my $row;
    my $rowcache;
    my $max_rows = 1;
    my $array_load;
    my $array_qmark;

### Perform the connection using the Oracle driver
my $db = DBI->connect(   $dsn, 
			 $user, 
			 $pass)
    or die "Can't connect to Oracle database: $DBI::errstr\n";
    
	$db->{AutoCommit}    = 0;
	$db->{RaiseError}    = 1;
	$db->{ora_check_sql} = 0;
	$db->{RowCacheSize}  = 16;
	
    #my $sqlFile = $sql_location . $sql_filename;
    my $sqlFile = ${sql_location}."/".${sql_table}.".sql";
    
    #print $sqlFile;
    
	#load sql file
	open (SQL, "$sqlFile");
	    #or die (Can't open file "$sqlFile" for reading);
	
	my $array_ref;
	while (my $sqlStatement = <SQL>) {
	    
	    #remove lines that start with "--"
	    if ($sqlStatement =~ /^\s*\--/ ) {
		next;
	    }
	    
	    #replace variable if found
	    $sqlStatement =~ s/&FIC_MIS_DATE/$fic_mis_date/g;
	    
	    #push into array
	    push @{ $array_ref }, $sqlStatement;
	
	}
	
	#print final query to file for testing and log purposes (should be commented out in production)
	my $filename = ${sql_location}."/LOGS/".${sql_table}.'.txt';
	open(my $fh, '>', $filename) or die "Could not open file '$filename' $!";
	print $fh "@{ $array_ref }";
	close $fh;
	
	my $sth = $db->prepare("@{ $array_ref }")
	      or die (qq(Can't prepare "@{ $array_ref }"));
	      
	$sth->execute()
	    or die qq(Can't execute "@{ $array_ref }");
    
    #Connect to SQL SERVER for insert
    my $db2 = DBI->connect("$dsn_rho;Server=$host_rho;Database=$database_rho",
			    $user_rho,
			    $auth_rho,
			    { RaiseError => 1, AutoCommit => 1}
			  ) || die "Database connection not made: $DBI::errstr";
    
    #Clear existing table in SQL
    my $del = $db2->prepare("DELETE FROM [RHO].[dbo].[$sql_table]")
	    or die (qq(Can't prepare DELETE query for " . $sql_table "));
	    
       $del->execute()
	    or die qq(Can't execute DELETE " . $sql_table ");
	    
    #Get column names from table
    my $col = $db2->prepare("SELECT COLUMN_NAME FROM [RHO].[INFORMATION_SCHEMA].[COLUMNS] WHERE TABLE_NAME = '" . $sql_table . "'")
	    or die (qq(Can't prepare COLUMN query for " . $sql_table "));
	    
       $col->execute()
	    or die qq(Can't execute COLUMN " . $sql_table ");
	    
    #Push column names into array and count # of question marks would go into insert query (also push into array)
	no warnings;
        while(my $row = shift(@$rowcache) || shift(@{$rowcache=$col->fetchall_arrayref(undef, $max_rows)})) 
				{
		#print join(", ", values @{$row}), "\n";
		push @ { $array_load }, values @ { $row } ;
		push @ { $array_qmark }, '?' ;
				}		
	use warnings;
    
    #Prepare insert query into RHO database
    my $ins = $db2->prepare("INSERT INTO [RHO].[dbo].[$sql_table] (" . join(", ", values @ { $array_load }) . ") VALUES(" . join(", ", values @ { $array_qmark }) . ")");

	no warnings;
        while(my $row = shift(@$rowcache) || shift(@{$rowcache=$sth->fetchall_arrayref(undef, $max_rows)})) 
				{
				
		my $datestring = localtime();
		push @{ $row }, $datestring;

		foreach my $value (@{$row})
		{
			$value =~ s/([HASH]+)\(([^)]+)\)//g;
		}
		
		my @array = $row;
		
		my @tuple_status;
		$ins->execute_for_fetch( sub { shift @array }, \@tuple_status);
		
				}		
	use warnings;
	

	END {
	    $db->disconnect if defined($db);
	}
