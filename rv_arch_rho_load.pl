#!C:/Perl64/bin/perl.exe

use strict;
use warnings;
use DBI;
use Cwd;
use CGI qw(:standard);
use Data::Dumper;
use Time::Piece;

my $dsn = 'DBI:Oracle:server.domain.com:port/sname.domain.com';
my $database= 'xxxx';
my $user= 'xxxx';
my $pass = 'xxxx';

my $query = new CGI;

#declare variables:
#pick local directory
my $sql_location = cwd();
my $sql_table = $ARGV[0];
my $fic_mis_date = $ARGV[1];

    #connect to sql server and load data from reveleus prod
    my $dsn_rho = 'DBI:ODBC:Driver={SQL Server}';
    my $host_rho = 'xxxx\VEGA';
    my $database_rho = 'RHO';
    my $user_rho= '';
    my $auth_rho = '';
    
    #declare sub-elements		
    my $row;
    my $rowcache;
    my $max_rows = 1;
    my $tbl_array;
    my $array_load;
    my $array_qmark;

#####################################################################################
###Prepare systems

### Perform the connection using the Oracle driver
    my $db = DBI->connect(   $dsn, 
			     $user, 
			     $pass)
	or die "Can't connect to Oracle database: $DBI::errstr\n";
	
	    $db->{AutoCommit}    = 0;
	    $db->{RaiseError}    = 1;
	    $db->{ora_check_sql} = 0;
	    $db->{RowCacheSize}  = 16;
	
###Connect to SQL SERVER for insert
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
	        
    #If xxxx is not defined then identify all archived tables in order to loop throught them
    #If xxxx is defined then convert to archive format string and push into $tbl_array
        
    my $tblid;
    
    if ( defined $fic_mis_date ) {
	
	my $tbl_mis_date = Time::Piece->strptime($fic_mis_date, '%d-%b-%Y')->strftime('%Y%m%d');
	
	push @ { $tbl_array }, $tbl_mis_date;
	
    }
    else
    {
	
	$tblid = $db->prepare("
				 SELECT DISTINCT SUBSTR(TABLE_NAME, INSTR(TABLE_NAME, '_2', -1, 1) + 1, LENGTH(TABLE_NAME) - INSTR(TABLE_NAME, '_2', -1, 1)) 
				 FROM ALL_TAB_COLUMNS 
				 WHERE OWNER = 'xxxx' AND TABLE_NAME LIKE 'xxxx_%'
				 AND 
				 SUBSTR(TABLE_NAME, INSTR(TABLE_NAME, '_2', -1, 1) + 1, LENGTH(TABLE_NAME) - INSTR(TABLE_NAME, '_2', -1, 1)) <> '20130930'
				 ORDER BY SUBSTR(TABLE_NAME, INSTR(TABLE_NAME, '_2', -1, 1) + 1, LENGTH(TABLE_NAME) - INSTR(TABLE_NAME, '_2', -1, 1)) DESC
				 ")
		or die (qq(Can't prepare ARCH TABLES query));

	$tblid->execute()
	    or die qq(Can't find archived tables);
	
	no warnings;
        while($row = shift(@$rowcache) || shift(@{$rowcache=$tblid->fetchall_arrayref(undef, $max_rows)})) 
				{
		#print join("|", values @{$row}), "\n";
		push @ { $tbl_array }, values @ { $row };
				}
	use warnings;
	
    }
	
    my $sqlFile = ${sql_location}."/".${sql_table}.".sql";
	
    #Get column names from table
    my $col = $db2->prepare("SELECT COLUMN_NAME FROM [RHO].[INFORMATION_SCHEMA].[COLUMNS] WHERE TABLE_NAME = '" . $sql_table . "'")
	    or die (qq(Can't prepare COLUMN query for " . $sql_table "));
	    
       $col->execute()
	    or die qq(Can't execute COLUMN " . $sql_table ");
	    
    #Push column names into array and count # of question marks would go into insert query (also push into array)
	no warnings;
        while($row = shift(@$rowcache) || shift(@{$rowcache=$col->fetchall_arrayref(undef, $max_rows)})) 
				{
		#print join(", ", values @{$row}), "\n";
		push @ { $array_load }, values @ { $row } ;
		push @ { $array_qmark }, '?' ;
				}		
	use warnings;
	
#####################################################################################	    
	
#####################################################################################
#START LOOP
#####################################################################################

foreach (@{$tbl_array}) {

#print "##############################################################################\n";
	
	#load sql file
	open (SQL, "$sqlFile");
	    #or die (Can't open file "$sqlFile" for reading);
	
	my $array_ref;
	while (my $sqlStatement = <SQL>) {
	    
	    #remove lines that start with "--"
	    if ($sqlStatement =~ /^\s*\--/ ) {
		next;
	    }
	    
	    #replace table name variable if found
	    $sqlStatement =~ s/&TBL_DATE/$_/g;
	    #replace fic_mis_date variable if found
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

    #Prepare insert query into RHO database    
    my $ins = $db2->prepare("INSERT INTO [RHO].[dbo].[$sql_table] (" . join(", ", values @ { $array_load }) . ") VALUES(" . join(", ", values @ { $array_qmark }) . ")");
    
	no warnings;
        while($row = shift(@$rowcache) || shift(@{$rowcache=$sth->fetchall_arrayref(undef, $max_rows)})) 
				{

		my $datestring = localtime();
		push @{ $row }, $datestring;
		
		foreach my $row (@{$row})
		{
			$row =~ s/([HASH]+)\(([^)]+)\)//g;
		}
		
		#print join("|", values @{$row}), "\n";
		
		my @array = $row;
		
		my @tuple_status;
		$ins->execute_for_fetch( sub { shift @array }, \@tuple_status);
		
				}
		
				
	use warnings;

    }
#####################################################################################
#END LOOP
#####################################################################################	    	

	END {
	    $db->disconnect if defined($db);
	}
