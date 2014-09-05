#!C:/Perl64/bin/perl.exe

  use strict;
  use warnings;
  use DateTime;
  use Cwd;
  use DBI;
  use threads;
  use threads::shared;

#$SIG{CHLD} = 'IGNORE';

my $script_location = cwd();

#my $pqrt = '_20130930'; #'31-DEC-2013'; #'_20131231';

my $pqrt = '30-JUN-2014';
my $ptbl01 = 'q1';
my $ptbl02 = 'q2';
my $ptbl03 = 'q3';
my $ptbl04 = 'q4';
my $ptbl1 = 'pq5';
my $ptbl2 = 'pq6';
my $ptbl3 = 'pq7';
my $ptbl4 = 'pq8';
my $ptbl5 = 'pq9';
my $ptbl6 = 'pq10';
my $ptbl7 = 'pq11';

my $prod_filter01 = cwd().'\rv_prod_rho_load.pl '.${ptbl01}." ".${pqrt};
my $prod_filter02 = cwd().'\rv_prod_rho_load.pl '.${ptbl02}." ".${pqrt};
my $prod_filter03 = cwd().'\rv_prod_rho_load.pl '.${ptbl03}." ".${pqrt};
my $prod_filter04 = cwd().'\rv_prod_rho_load.pl '.${ptbl04}." ".${pqrt};
my $prod_filter1 = cwd().'\rv_prod_rho_load.pl '.${ptbl1}." ".${pqrt};
my $prod_filter2 = cwd().'\rv_prod_rho_load.pl '.${ptbl2}." ".${pqrt};
my $prod_filter3 = cwd().'\rv_prod_rho_load.pl '.${ptbl3}." ".${pqrt};
my $prod_filter4 = cwd().'\rv_prod_rho_load.pl '.${ptbl4}." ".${pqrt};
my $prod_filter5 = cwd().'\rv_prod_rho_load.pl '.${ptbl5}." ".${pqrt};
my $prod_filter6 = cwd().'\rv_prod_rho_load.pl '.${ptbl6}." ".${pqrt};
my $prod_filter7 = cwd().'\rv_prod_rho_load.pl '.${ptbl7}." ".${pqrt};

my $aqrt = '31-MAR-2014';
my $atbl01 = 'aq1';
my $atbl02 = 'aq2';
my $atbl03 = 'aq3';
my $atbl04 = 'aq4';
my $atbl1 = 'apq5';
my $atbl2 = 'apq6';
my $atbl3 = 'apq7';
my $atbl4 = 'apq8';
my $atbl5 = 'apq9';
my $atbl6 = 'apq10';
my $atbl7 = 'apq11';

my $arch_filter01 = cwd().'\rv_arch_rho_load.pl '.${atbl01}." ".${aqrt};
my $arch_filter02 = cwd().'\rv_arch_rho_load.pl '.${atbl02}." ".${aqrt};
my $arch_filter03 = cwd().'\rv_arch_rho_load.pl '.${atbl03}." ".${aqrt};
my $arch_filter04 = cwd().'\rv_arch_rho_load.pl '.${atbl04}." ".${aqrt};
my $arch_filter1 = cwd().'\rv_arch_rho_load.pl '.${atbl1}." ".${aqrt};
my $arch_filter2 = cwd().'\rv_arch_rho_load.pl '.${atbl2}." ".${aqrt};
my $arch_filter3 = cwd().'\rv_arch_rho_load.pl '.${atbl3}." ".${aqrt};
my $arch_filter4 = cwd().'\rv_arch_rho_load.pl '.${atbl4}." ".${aqrt};
my $arch_filter5 = cwd().'\rv_arch_rho_load.pl '.${atbl5}." ".${aqrt};
my $arch_filter6 = cwd().'\rv_arch_rho_load.pl '.${atbl6}." ".${aqrt};
my $arch_filter7 = cwd().'\rv_arch_rho_load.pl '.${atbl7}." ".${aqrt};

my $sqlsrv = 'x_loadstatus';

print "Starting main program\n";

my @threads;
my $count = 2;

	my $pt01 = threads->new(\&psub01, 1);
	my $pt02 = threads->new(\&psub02, 2);
	my $pt03 = threads->new(\&psub03, 3);
	my $pt04 = threads->new(\&psub04, 4);
        my $pt1 = threads->new(\&psub1, 4);
        my $pt2 = threads->new(\&psub2, 5);
	my $pt3 = threads->new(\&psub3, 6);
	my $pt4 = threads->new(\&psub4, 7);
	my $pt5 = threads->new(\&psub5, 8);
	my $pt6 = threads->new(\&psub6, 9);
	my $pt7 = threads->new(\&psub7, 10);

	my $at01 = threads->new(\&asub01, 11);
	my $at02 = threads->new(\&asub02, 12);
	my $at03 = threads->new(\&asub03, 13);
	my $at04 = threads->new(\&asub04, 14);	
	my $at1 = threads->new(\&asub1, 14);
	my $at2 = threads->new(\&asub2, 15);
	my $at3 = threads->new(\&asub3, 16);
	my $at4 = threads->new(\&asub4, 17);
	my $at5 = threads->new(\&asub5, 18);
	my $at6 = threads->new(\&asub6, 19);
	my $at7 = threads->new(\&asub7, 20);
	
	push(@threads,$pt01);
	push(@threads,$pt02);
	push(@threads,$pt03);
	push(@threads,$pt04);
        push(@threads,$pt1);
        push(@threads,$pt2);
	push(@threads,$pt3);
	push(@threads,$pt4);
	push(@threads,$pt5);
	push(@threads,$pt6);
	push(@threads,$pt7);

	push(@threads,$at01);
	push(@threads,$at02);
	push(@threads,$at03);
	push(@threads,$at04);	
	push(@threads,$at1);
	push(@threads,$at2);
	push(@threads,$at3);
	push(@threads,$at4);
	push(@threads,$at5);
	push(@threads,$at6);
	push(@threads,$at7);

foreach (@threads) {
        my $num = $_->join;
        #print "done with $num\n";
}
print "End of main program\n";

sub psub01 {
        my $num = shift;
        print "started thread $ptbl01 - $num\n";
        system(${prod_filter01});
	#print $prod_filter1;
        print "\ndone with thread $ptbl01 - $num\n";
        return $num;
}

sub psub02 {
        my $num = shift;
        print "started thread $ptbl02 - $num\n";
        system(${prod_filter02});
	#print $prod_filter1;
        print "\ndone with thread $ptbl02 - $num\n";
        return $num;
}

sub psub03 {
        my $num = shift;
        print "started thread $ptbl03 - $num\n";
        system(${prod_filter03});
	#print $prod_filter1;
        print "\ndone with thread $ptbl03 - $num\n";
        return $num;
}

sub psub04 {
        my $num = shift;
        print "started thread $ptbl04 - $num\n";
        system(${prod_filter04});
	#print $prod_filter1;
        print "\ndone with thread $ptbl04 - $num\n";
        return $num;
}

sub psub1 {
        my $num = shift;
        print "started thread $ptbl1 - $num\n";
        system(${prod_filter1});
	#print $prod_filter1;
        print "\ndone with thread $ptbl1 - $num\n";
        return $num;
}

sub psub2 {
        my $num = shift;
        print "started thread $ptbl2 - $num\n";
        system(${prod_filter2});
	#print $prior_filter;
        print "\ndone with thread $ptbl2 - $num\n";
        return $num;
}

sub psub3 {
        my $num = shift;
        print "started thread $ptbl3 - $num\n";
        system(${prod_filter3});
	#print $prior_filter;
        print "\ndone with thread $ptbl3 - $num\n";
        return $num;
}

sub psub4 {
        my $num = shift;
        print "started thread $ptbl4 - $num\n";
        system(${prod_filter4});
	#print $prior_filter;
        print "\ndone with thread $ptbl4 - $num\n";
        return $num;
}

sub psub5 {
        my $num = shift;
        print "started thread $ptbl5 - $num\n";
        system(${prod_filter5});
	#print $prior_filter;
        print "\ndone with thread $ptbl5 - $num\n";
        return $num;
}

sub psub6 {
        my $num = shift;
        print "started thread $ptbl6 - $num\n";
        system(${prod_filter6});
	#print $prior_filter;
        print "\ndone with thread $ptbl6 - $num\n";
        return $num;
}

sub psub7 {
        my $num = shift;
        print "started thread $ptbl7 - $num\n";
        system(${prod_filter7});
	#print $prior_filter;
        print "\ndone with thread $ptbl7 - $num\n";
        return $num;
}

sub asub01 {
        my $num = shift;
        print "started thread $atbl01 - $num\n";
        system(${arch_filter01});
	#print $curr_filter;
        print "\ndone with thread $atbl01 - $num\n";
        return $num;
}

sub asub02 {
        my $num = shift;
        print "started thread $atbl02 - $num\n";
        system(${arch_filter02});
	#print $curr_filter;
        print "\ndone with thread $atbl02 - $num\n";
        return $num;
}

sub asub03 {
        my $num = shift;
        print "started thread $atbl03 - $num\n";
        system(${arch_filter03});
	#print $curr_filter;
        print "\ndone with thread $atbl03 - $num\n";
        return $num;
}

sub asub04 {
        my $num = shift;
        print "started thread $atbl04 - $num\n";
        system(${arch_filter04});
	#print $curr_filter;
        print "\ndone with thread $atbl04 - $num\n";
        return $num;
}

sub asub1 {
        my $num = shift;
        print "started thread $atbl1 - $num\n";
        system(${arch_filter1});
	#print $curr_filter;
        print "\ndone with thread $atbl1 - $num\n";
        return $num;
}

sub asub2 {
        my $num = shift;
        print "started thread $atbl2 - $num\n";
        system(${arch_filter2});
	#print $curr_filter;
        print "\ndone with thread $atbl2 - $num\n";
        return $num;
}


sub asub3 {
        my $num = shift;
        print "started thread $atbl3 - $num\n";
        system(${arch_filter3});
	#print $curr_filter;
        print "\ndone with thread $atbl3 - $num\n";
        return $num;
}

sub asub4 {
        my $num = shift;
        print "started thread $atbl4 - $num\n";
        system(${arch_filter4});
	#print $curr_filter;
        print "\ndone with thread $atbl4 - $num\n";
        return $num;
}

sub asub5 {
        my $num = shift;
        print "started thread $atbl5 - $num\n";
        system(${arch_filter5});
	#print $curr_filter;
        print "\ndone with thread $atbl5 - $num\n";
        return $num;
}

sub asub6 {
        my $num = shift;
        print "started thread $atbl6 - $num\n";
        system(${arch_filter6});
	#print $curr_filter;
        print "\ndone with thread $atbl6 - $num\n";
        return $num;
}

sub asub7 {
        my $num = shift;
        print "started thread $atbl7 - $num\n";
        system(${arch_filter7});
	#print $curr_filter;
        print "\ndone with thread $atbl7 - $num\n";
        return $num;
}

    #connect to sql server and load data from oracle db
    my $dsn_rho = 'DBI:ODBC:Driver={SQL Server}';
    my $host_rho = 'xxxx\VEGA';
    my $database_rho = 'RHO';
    my $user_rho= '';
    my $auth_rho = '';
        
    #Connect to SQL SERVER for insert
    my $db = DBI->connect("$dsn_rho;Server=$host_rho;Database=$database_rho",
			    $user_rho,
			    $auth_rho,
			    { RaiseError => 1, AutoCommit => 1}
			  ) || die "Database connection not made: $DBI::errstr";	
    
    #my $sqlFile = $sql_location . $sql_filename;
    my $sqlFile = ${script_location}.'/'.$sqlsrv.'.sql';
    
    print $sqlFile;
    
	#load sql file
	open (SQL, "$sqlFile");
	    #or die (Can't open file "$sqlFile" for reading);
	
	my $array_ref;
	while (my $sqlStatement = <SQL>) {
	    
	    #remove lines that start with "--"
	    if ($sqlStatement =~ /^\s*\--/ ) {
		next;
	    }
	    
	    #push into array
	    push @{ $array_ref }, $sqlStatement;
	
	}
    
    	my $filename = ${script_location}.'/LOGS/'.$sqlsrv.'.txt';
	open(my $fh, '>', $filename) or die "Could not open file '$filename' $!";
	print $fh "@{ $array_ref }";
	close $fh;
    
    my $sth = $db->prepare("@{ $array_ref }")
	or die (qq(Can't prepare "@{ $array_ref }"));
	      
	$sth->execute()
	    or die qq(Can't execute "@{ $array_ref }");    
        
print "\nExecuted: $sqlFile\n";
print "\nTables on SQL Server were updated\n";
    
