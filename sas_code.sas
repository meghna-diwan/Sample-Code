/*************************************************************************;
%** PROGRAM: useful_sas_procedures
%** PURPOSE: Document Useful SAS Procedures
%** NOTES:
%** AUTHOR: Meghna Diwan   
%** DATE CREATED: 05/23/2019
%** DATE LAST MODIFIED: 05/23/2019
%*************************************************************************/

%MACRO useful_sas_procedure();

   /***********************************************************************************/
   /* 1:M MERGE USING HASH */
   /***********************************************************************************/

   data OUTPUT_FILE;
      length ;
      format ;   
      call missing(A, B, C, L, M, N); 
      if _N_=1 then do;
         declare hash HASH1(dataset: "", multidata: "Y");
            HASH1.definekey("X", "Y");
            HASH1.definedata("A", "B", "C");
            HASH1.definedone();
         declare hash HASH2(dataset: "");
            HASH2.definekey("Z");
            HASH2.definedata("L", "M", "N");
            HASH2.definedone();   
      end;
      set INPUT_FILE;
      _rc=HASH.find(key:"x"); /*incase the merge variables dont have the same name*/
      if _rc^= 0 then output;
      do while(_rc=0);               
         if HASH2.find()=0 then;        
         output;               
         _rc=HASH1.find_next();
      end;           
   run;
   
   /***********************************************************************************/
   /* SUMMARIZE   DISTRIBUTION */
   /***********************************************************************************/   
   
   proc means data=INPUT_FILE;
      class level1 level2; %* levels by which to summarize distribution;
      var dist_var; %* variable to find distribution;
      output out=OUTPUT_FILE
             N= mean= min= P1= P5= P25= median= P75= P90= P95= P99= max= nmiss= / autoname;
   run;
   
   /***********************************************************************************/
   /* SUMMARIZE ALL LEVELS OF VARS USING DYNAMIC HASH */
   /***********************************************************************************/

   %let level_var_list = var1 var2 var3 var4 var5; %*level vars;     
      %let hash_keys_0 = state; %*constant vars;
      %let hash_title_0=_hash_0;
      %do i=1 %to %eval(2**&nlevel_vars.-1);
         %let hash_keys_&i.=;
         %let hash_title_&i.=_hash_&i.;
         %do j=0 %to %eval(&nlevel_vars.-1);
            %let et=%eval(&j.+1);
            %if %sysfunc(mod(%sysfunc(floor(%eval(&i./2**&j.))),2))=1 %then %do;
               %let hash_keys_&i.=&&hash_keys_&i.. &&level_vars&et..;
            %end;
         %end;
         %put Hash number &i. has keys: &&hash_keys_&i..;
         %put Hash number &i. has title: &&hash_title_&i..;
   %end;


   data _null_;
     length state $2. n_rec 8.;
     call missing(state, n_rec);
     if _N_=1 then do;
        *Initialize all the variables to 0;
        n_rec=0;

        *Initialize the hashes;
        *Totals Hash for each state;
        declare hash hash_0(dataset: "work_&st..&st.&hash_title_0._&p.");
        hash_0.defineKey("state");
        hash_0.defineData("state", "n_rec");
        hash_0.defineDone();

        *Loop through each hash and initialize;
        %do h=1 %to %eval(2**&nlevel_vars.-1);
           declare hash hash_&h.(dataset: "work_&st..&st.&&hash_title_&h.._&p.");
           hash_&h..defineKey("state", %clist(%quotelst(&&hash_keys_&h..)));
           hash_&h..defineData("state", %clist(%quotelst(&&hash_keys_&h.)), "n_rec");
           hash_&h..defineDone();
        %end;
     end;

     set INPUT_FILE end=last_obs;

     *Do the counting here;
     %do h=0 %to %eval(2**&nlevel_vars.-1);
        if hash_&h..find() ^= 0 then do;
           n_rec=1;
           hash_&h..add();
        end;
        else do;
           n_rec=n_rec+1;
           hash_&h..replace();
        end;
     %end;

     if last_obs then do;
        %do h=0 %to %eval(2**&nlevel_vars.-1);
           hash_&h..output(dataset: "work_&st..&st.&&hash_title_&h.._&p.");
        %end;
     end;
   run;
   
   /***********************************************************************************/
   /* FIND DATASETS THAT HAVE AT LEAST 1 OBS */
   /***********************************************************************************/
   
   %do y=1 %to &nyear.; %let yr = &&year&y..;
      %do t=1 %to &nlist1.; %let l1=&&list1&t..; 
         %do f=1 %to &nlist2.; %let l2=&&list2&f..;
            %let exist_&list1._&list2. =0;
            %if %sysfunc(fileexist(FILEPATH)) %then %do; %* if that combination is found;
               libname &l2._&l1._&yr. "FILEPATH";
               %let exist_&list1._&list2. = %nobs(LIBNAME.DATASETNAME); %* no. of observations;
            %end; %*IF %SYSFUNC;
         %end; %*LIST2;
      %end; %*LIST1;   
   %end; %*YEAR;
   
   /***********************************************************************************/
   /* RENAME VARS */
   /***********************************************************************************/

   %*import crosswalk as SAS dataset;
   proc import datafile="VAR_CROSSWALK.xlsx" out=OUTPUTFILE dbms=xlsx replace;
   sheet = "SHEETNAME";
   run;

   %*create two lists to store two versions variable names, they will naturally be in the same order;
   proc sql noprint; select upcase(COL1) into: col1_name_list separated by " " from OUTPUTFILE;  quit;
   proc sql noprint; select upcase(COL2) into: col2_name_list separated by " " from OUTPUTFILE;  quit;
   %if &ncol1. ^= &ncol2. %then %do;
   %put !!!Warning: number of variable names dont match between Col1 and Col2;
   endsas;
   %end;

   %*rename all variables;
   proc datasets library=out;
   modify INPUTFILE;
      %do v=1 %to &ncol1_name.; %let name_col1=%upcase(&&col1_name&v..); %let name_col2=%upcase(&&col2_name&v..);
         %if &name_col1.^=&name_col2. %then rename &name_col2.=&name_col1.;;
      %end;
   run;
   
   /***********************************************************************************/
   /* NESTED PROC SQL TO SUMMARIZE DATA w FLAGS FOR ALL LEVELS */
   /***********************************************************************************/
   
   proc sql;
   create table OUTPUT_FILE as
   %** different levels ;
   %do h=1 %to %eval(2**&nlevel_vars.-1);
      select
        var_type, %clist(&hash_keys_0.), %if &v.=1 %then "valid" as flag3,; %if &v.=2 %then "_ALL_" as flag3,;
        %do l=1 %to &nlevel_vars.;
          %if %index(&&hash_keys_&h.., &&level_vars&l..) %then &&level_vars&l..,; %else "_ALL_" as &&level_vars&l.. ,;
        %end;
        count(distinct case when name^="Missing" %if &v.=1 %then %do; and (flag1=1 or flag2=1) %end; then name else "" end) as n_name,
        calculated n_name / calculated n_name as p_name,        
      from (select
            var_type, %clist(&hash_keys_0.), name, flag1, flag2,
            %do l=1 %to &nlevel_vars.;
               %if %index(&&hash_keys_&h.., &&level_vars&l..) %then &&level_vars&l..,; %else "_ALL_" as &&level_vars&l..,;
            %end;                     
            count(distinct case when name2^="Missing" %if &v.=1 %then %do; and (flag1=1 or flag2=1) %end; then name2 else "" end) as n_name2
            from work.&st._&YOS._id_collapse
            group by var_type, %clist(&hash_keys_0.), name, flag1, flag2, %clist(&&hash_keys_&h..))
            
      group by var_type, %clist(&hash_keys_0.), %clist(&&hash_keys_&h..)                      
      union
   %end;
   %*******;
   %** _ALL_ level;
   select
      var_type, %clist(&hash_keys_0.), %if &v.=1 %then "valid" as flag3,; %if &v.=2 %then "_ALL_" as flag3,;
      %do l=1 %to &nlevel_vars.;
         "_ALL_" as &&level_vars&l..,
      %end;
      count(distinct case when name^="Missing" %if &v.=1 %then %do; and (flag1=1 or flag2=1) %end; then name else "" end) as n_name,
      calculated n_name / calculated n_name as p_name,      
   from (select
            var_type, %clist(&hash_keys_0.), name, flag1, flag2,
            count(distinct case when name2^="Missing" %if &v.=1 %then %do; and (flag1=1 or flag2=1) %end; then name2 else "" end) as n_name2
          from work.&st._&YOS._id_collapse
          group by var_type, %clist(&hash_keys_0.), name, flag1, flag2)

   group by var_type, %clist(&hash_keys_0.)
   %****************;
   ;
   quit;
   
   /***********************************************************************************/
   /* DATASET ARRAY */
   /***********************************************************************************/
   
   data OUTPUT_FILE ;
      length ;            
      set INPUT_FILE;                                  
      array pmt_vars[&nadd_pmt_vars.] %do v=1 %to &nadd_pmt_vars.; &&add_pmt_vars&v. %end;;
      array pmt_new[&nadd_pmt_vars.] %do v=1 %to &nadd_pmt_vars.; z_&&add_pmt_vars&v. %end;;
      array strip[&nadd_pmt_vars.] %do v=1 %to &nadd_pmt_vars.; _s_&&add_pmt_vars&v. %end;;
      array flag[&nadd_pmt_vars.] %do v=1 %to &nadd_pmt_vars.; _f_&&add_pmt_vars&v. %end;;
      do _i=1 to &nadd_pmt_vars.;               
         pmt_new[_i] = pmt_vars[_i];               
         if missing(pmt_vars[_i]) then pmt_new[_i] = 0;
         strip[_i]=compress(put(pmt_new[_i], best32.));         
         flag[_i] = (lengthn(compress(strip[_i], "8."))=0);         
         _8_fill_first = findc(strip[_i],".")-1;          
         _8_fill_last = lengthn(substr(strip[_i], findc(strip[_i],".")+1,lengthn(strip[_i])-findc(strip[_i],".")));         
         if flag[_i]=1 then do;
            if _8_fill_first> 2 then pmt_new[_i] = 0;
            if _8_fill_first=-1 and _8_fill_last > 2then pmt_new[_i] = 0;
         end;                         
      end;                      
   run; 

   /***********************************************************************************/
   /* SUMMARIZE ALL LEVELS OF VARS USING PROC MEANS */
   /***********************************************************************************/
   
   proc means noprint completetypes
      data=INPUT_FILE;
      class level1 level2 level3 level4 level5/ preloadfmt; 
      var n_rec pmt %end;;
      output out=OUTPUT_FILE sum(pmt)= ;
      format pmt comma32.4;               
   run; 
   
   /***********************************************************************************/
   /* IMPORT DATA FROM CSV */
   /***********************************************************************************/
   
   proc import datafile="INPUT FILE PATH" 
      out=OUTPUT_FILE
      dbms=csv replace; 
      getnames=no; %* use names from data;
      datarow=2;
   run;
   
   /***********************************************************************************/
   /* RESHAPE LONG TO WIDE */ (UNIQUE 
   /***********************************************************************************/
   
   %* find array size of the long data;
   data _null_;
      set INPUT_FILE end=last;
      by var1;
      retain max_counter counter 0;
      if first.var1 then counter=0;
      counter=counter+1;
      max_counter=max(max_counter,counter);
      if last=1 then call symputx("array_size", max_counter);
   run;

   data WIDE_INPUT_FILE;      
      set INPUT_FILE;
      by var1 var2;  
      array var2_{&array_size.} $15.;
      retain var2_1-var2_&array_size. counter;
      if first.var1 then do;
         counter=0;              
         call missing(of var2_{*});    
      end;
      counter=counter+1;
      var2_{counter}=var2;      
      if last.var1 then output;            
   run;
   
   /***********************************************************************************/
   /* DELETE DATASET */
   /***********************************************************************************/
   
   proc delete data=INPUT_FILE (gennum=all);
   run;
   
   /***********************************************************************************/
   /* DELETE FOLDERS */
   /***********************************************************************************/

   filename dir "[path of the folder you want to delete]";
   data _null_;
      rc = fdelete("dir");
      /*for debug;
      put rc=;
      msg = sysmsg();
      put msg=; */
   run;         

   /***********************************************************************************/
   /* COPY DATASET */
   /***********************************************************************************/

   proc copy in=INPUT_LIBNAME out=OUTPUT_LIBNAME memtype=data;
      select INPUT_DATA OUTPUT_DATA;
   run;

   /***********************************************************************************/
   /* RENAME DATASET */
   /***********************************************************************************/

   proc datasets library = LIBNAME;
      change OLD_NAME = NEW_NAME;
   run;

   /***********************************************************************************/
   /* RANK ROWS BY VAR1(descending) */
   /***********************************************************************************/

   proc rank data=INPUT_FILE out=OUTPUT descending;
      var var1;
      ranks rank_var;
   run;

   /***********************************************************************************/
   /*MERGE USING PROC SQL : KEEP OBS/VARS IN BOTH */
   /***********************************************************************************/
   
   proc sql;
   create table MERGE_FILE as
   select
     coalesce(a.var1, b.var1) as var1,
     var2, var3, var4, var5
   from INPUT_FILE_1 a
   full join INPUT_FILE_2. b
   on a.var1 = b.var1;
   quit;


   proc sql;
   create table MERGE_FILE as
   select 
   %*key variables used for merge to identify unique claims;
   %do iv=1 %to &nc_id_vars.; %let id_var=&&c_id_vars&iv..;
      coalesce(date1.&id_var., data2.&id_var.) as &id_var.,
   %end;

   %*variables found in both data1 and data2;
   %do l=1 %to &nlevels.; %let lvl=&&levels&l..;                                           
      %do kv=1 %to &nc_keep_vars.; %let keep_var=&&c_keep_vars&kv..;
         %if "&lvl."="data1" %then %do;
            data1.&keep_var. as &keep_var._&data1.,
         %end;
         %else %if "&lvl."="data2" %then %do;
            data2.&keep_var. as &keep_var._&data2.,                        
         %end; 
      %end;               
   %end;

   %*data1 variables to keep;
   %do hv=1 %to &nc_data1_vars.; %let data1_var=&&c_data1_vars&hv..;
      data1.&data1_var.,
   %end;

   %*data2 variables to keep;
   %do lv=1 %to &nc_data2_vars.; %let data2_var=&&c_data2_vars&lv..;
      data2.&data2_var.,
   %end;

   %*flag data2 without corresponding data1;
   case when 1
      %do iv=1 %to &nc_id_vars.; %let id_var=&&c_id_vars&iv..;               
         and data2.&id_var. is null   
      %end;
      then "0"  else "1" end as in_data2, 

   %*flag data1 without corresponding data2;
   case when 1 
      %do iv=1 %to &nc_id_vars.; %let id_var=&&c_id_vars&iv..;               
         and data1.&id_var. is null   
      %end;
      then "0"  else "1" end as in_data1, 

   %*flag merged data1/data2;
   case when 1
      %do iv=1 %to &nc_id_vars.; %let id_var=&&c_id_vars&iv..;               
         and data1.&id_var.=data2.&id_var.
      %end;                     
      then "1" else "0" end as in_both 

   from INPUT_FILE_1 as data2
   full join INPUT_FILE_2 as data1
   on 1
   %do iv=1 %to &nc_id_vars.; %let id_var=&&c_id_vars&iv..;               
      and data1.&id_var.=data2.&id_var.
   %end; 
   ; 
   quit;


%MEND: