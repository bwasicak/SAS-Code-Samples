/**************************************************************************************
***   Build plan
***   1st - everytime the pgm runs it needs to log itself and update a master SAS dsn with date and time.
***   2nd - It needs to read a specific set of directories
***   3rd - it needs to run once a day
***   4th - it needs to email me the results
***   5th - FR req.'s
***         1. capture stats with date time stamp
***         2. report on the top 10 largest 
***************************************************************************************/
options obs=max ls=140 nocenter replace=yes missing='' nospool bufsize=256k ubufsize=256k  bufno=16;

%let _tdate=%sysfunc(putn("&sysdate9"d,worddate20.));
%let _fdate=%sysfunc(putn("&sysdate9"d,yymmddn8.));

libname outdd1 '/data/common/data';
run;   

%macro secondFile(_file=);

%sysexec du -s &_dir/&&file_name&i >> DUfileList&i..txt;

data duHome(drop=x y line1 /* _size */ ); 
  retain totSpace 0;
  infile "DUfileList&i..txt" missover lrecl=32767 length=l end=eof;  
  attrib _size       length=$12.;
  attrib _directory  length=$200.;
  attrib space       length=8 format=best24. informat=best24.;
  attrib FileSystemSpace length=$10.;
  attrib FileSystem  length=$20.;
 
   input @ ;
   input line1 $varying200. l;
   x=length(line1);
   y=index(line1,'/');
   if line1 in('.','..') then delete;
   _size=substr(line1,1,y-1);
   _directory=substr(line1,y,x-1);
   space=input(_size,best12.)*1000;
   totSpace=totSpace+space;

   FileSystemSpace="&_dirSize";
   FileSystem="&fullpath";

   _runDate=today();
   _runTime=put(time(),hhmm.);
   *if eof then do;
      call symput('totSpace',put(totSpace,sizekmg.2));
   *end;
run; 

%put "total space for &&file_name&i   is &totSpace     parent &_dir ";

* proc print data=duhome;
*   format space sizekmg.2;
* run;

* proc append base=outdd1.MasterDataUse data=duHome FORCE ;  *run;
data outdd1.MasterDataUse;
   set  duHome 
        outdd1.MasterDataUse;
run;

%sysexec rm DUfileList&i..txt;
%mend secondFile;                                       /* end of the secondFile macro  */



*** read all directories in /data/common  ***;
%macro firstRead(_dir=);
%local fullpath; 
%let fullpath=&_dir;

%sysexec ls -ltra &_dir > dirRead.txt ;
%sysexec du -sh &_dir > dirtotal.txt ;


*** READ THE PARENT DIRECORTY TO GET THE TOTAL SPACE ***; 
data _null_;
  infile 'dirtotal.txt' missover lrecl=32767 length=l end=eof firstobs=1;
  attrib _dirSize    length=$10.
         _dirname    length=$25.
   ;

   input @ ;
   input line1 $varying200. l;
   if line1 in('.','..') then delete;  
   
   array _elements(*) _dirSize _dirname;
   do i=1 to dim(_elements) ;
      _elements(i)=scan(line1,i,'/');
   end; 

   call symput('_dirSize',_dirSize);
run; 

%put "this is dirSize  &_dirSize" ;

*** READ THE DIRECTORY LISTING ***;
data _null_;
  infile 'dirRead.txt' missover lrecl=32767 length=l end=eof firstobs=2;
  retain cntr 0;
  attrib _permission length=$10.
         x           length=$1.
         _owner      length=$25.
         _group      length=$25.
         _size       length=$4.
         _month      length=$3.
         _day        length=$2.
         _year       length=$4.
         _something  length=$25.
   ;

   input @ ;
   input line1 $varying200. l;
   if line1 in('.','..') then delete;

    array _elements(*) _permission x _owner _group _size _month _day _year _something;
    do i=1 to dim(_elements) ;
      _elements(i)=scan(line1,i,' ');
    end;
 
   if substr(_something,1,1)='.' then delete;
   if substr(_permission,1,1)='d' then do;
    ***-----------------------------------------------***;
    *** create macro variables for the file directory ***;
    ***-----------------------------------------------***;
    cntr+1 ;
    call symput('file_name'||trim(left(put(cntr,3.))),_something);
   end;
  ***------------------------------------------***;
  *** create macro variable maximum file count ***;
  ***------------------------------------------***;

  if eof then do;
    put 'number of files in directory ' _n_ ;
  *  call symput('file_max_cntr',trim(left(put(cntr,5.))) );
  end;
   call symput('file_max_cntr',trim(left(put(cntr,5.))) ); 
run; 

%do i= 1 %to &file_max_cntr;
  %put &&file_name&i; 
    %secondFile(_file=&&file_name&i);
%end;


***  CLEAN UP THE TXT FILES  ***; 
%sysexec rm dirRead.txt;  
%sysexec rm dirtotal.txt;
%mend firstRead;

%firstRead(_dir=/data/common);  
%firstRead(_dir=/data/bi/crm);
%firstRead(_dir=/data/bi/uscb);
%firstRead(_dir=/data/adva);
%firstRead(_dir=/data/general);

*** DEDUPING THE MASTER DATASET ***;
proc sort data=outdd1.MasterDataUse nodupkey;
  by _directory _runDate;  *  _runTime;
run;


*** SELECTING TODAY'S TOP SPACE USERS ***;
proc sort data=outdd1.MasterDataUse out=MasterDataUse;
 where _runDate=today();
 by descending space _directory _runDate _runTime;
run;


PROC TEMPLATE;
  DEFINE STYLE myocean; 
    PARENT=styles.ocean;
    REPLACE TABLE FROM OUTPUT /
    FRAME = void
    RULES = rows
    CELLPADDING = 3pt
    CELLSPACING = 0.0pt
    BORDERWIDTH = 0.2pt;
  END;
RUN; 

ODS PDF FILE="/data/common/reports/Egsas04p_DataUse_&_fdate..pdf" STYLE=myocean;

proc print data=MasterDataUse(obs=20)  obs='*Rank' label split='*';
  title "Top 20 directorys for &_tdate";
  label _directory='*directory'
        _size='directory*space use'
        ;
   var _directory space FileSystemSpace FileSystem _runDate _runTime; 
   format space sizekmg.2  _runDate yymmdd10.;
run;


ODS PDF CLOSE; 

****  put email here   ***; 
filename outbox email 'rwasicak@expedia.com' lrecl=125; 
run;

data _null_;
  file outbox
    /* Overrides value in filename statement */
     /* to=('bob.wasicak@orbitz.com') */ 
     subject='/Data Top 20 space users'
     type="TEXT/HTML" 
     attach="/data/common/reports/Egsas04p_DataUse_&_fdate..pdf" ; 
     put 'Bob,';
     put "/Data Top 10 spaceusers";
     put ///;
run;

endsas; 
