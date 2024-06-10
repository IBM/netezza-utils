#!/bin/bash

# Copyright IBM Corp. 2011, 2013, 2014
# LICENSE: See licenses provided at download or with distribution for language or\and locale-specific licenses

query_output=$(nzsql -admin -tc "
    SELECT    db.objname  AS \"Database\",
            sch.objname AS \"Schema\",
            substr(obj.objname, 1, strpos(obj.objname, '#') -1) AS \"UDX Name\",
            CASE WHEN obj.objclass = 4919 THEN 'FUNCTION' ELSE 'AGGREGATE' END AS \"Type\"

    FROM      _t_object obj
            INNER JOIN _t_object db  ON obj.objdb        = db.objid
            INNER JOIN _t_object sch ON obj.objschemaoid = sch.objid

    WHERE     obj.objid > 200000
    AND       obj.objclass IN (4917, 4919)

    ----------------------------------------

    AND       upper(\"UDX Name\") NOT IN
    (
    -- SQL Extensions Toolkit FUNCTIONS

        'ADD_ELEMENT', 'ARRAY', 'ARRAY_COMBINE', 'ARRAY_CONCAT', 'ARRAY_COUNT', 'ARRAY_SPLIT', 'ARRAY_TYPE',
        'COLLECTION', 'COMPRESS', 'COMPRESS_NVARCHAR', 'CRYPTO_VERSION', 'DAY', 'DAYS_BETWEEN', 'DECOMPRESS',
        'DECOMPRESS_NVARCHAR', 'DECRYPT', 'DECRYPT_NVARCHAR', 'DELETE_ELEMENT', 'ELEMENT_NAME', 'ELEMENT_TYPE',
        'ENCRYPT', 'ENCRYPT_NVARCHAR', 'FPE_DECRYPT', 'FPE_ENCRYPT', 'GET_VALUE_DATE', 'GET_VALUE_DOUBLE',
        'GET_VALUE_INT', 'GET_VALUE_NVARCHAR', 'GET_VALUE_TIME', 'GET_VALUE_TIMESTAMP', 'GET_VALUE_TIMETZ',
        'GET_VALUE_VARCHAR', 'GREATEST', 'HASH', 'HASH4', 'HASH8', 'HASH_NVARCHAR', 'HEXTORAW', 'HOUR',
        'HOURS_BETWEEN', 'ISVALIDXML', 'ISXML', 'LEAST', 'MINUTE', 'MINUTES_BETWEEN', 'MONTH', 'MT_RANDOM',
        'NARRAY_COMBINE', 'NARRAY_SPLIT', 'NELEMENT_NAME', 'NEXT_MONTH', 'NEXT_QUARTER', 'NEXT_WEEK',
        'NEXT_YEAR', 'RAWTOHEX', 'REGEXP_EXTRACT', 'REGEXP_EXTRACT_ALL', 'REGEXP_EXTRACT_ALL_SP',
        'REGEXP_EXTRACT_SP', 'REGEXP_INSTR', 'REGEXP_LIKE', 'REGEXP_MATCH_COUNT', 'REGEXP_REPLACE',
        'REGEXP_REPLACE_SP', 'REGEXP_VERSION', 'REPLACE', 'REPLACE_ELEMENT', 'SECOND', 'SECONDS_BETWEEN',
        'STRLEFT', 'STRRIGHT', 'THIS_MONTH', 'THIS_QUARTER', 'THIS_WEEK', 'THIS_YEAR', 'UUDECODE',
        'UUENCODE', 'WEEKS_BETWEEN', 'WORD_DIFF', 'WORD_FIND', 'WORD_KEY', 'WORD_KEYS_DIFF', 'WORD_KEY_TOCHAR',
        'WORD_STEM', 'XMLATTRIBUTES', 'XMLCONCAT', 'XMLELEMENT', 'XMLEXISTSNODE', 'XMLEXTRACT',
        'XMLEXTRACTVALUE', 'XMLPARSE', 'XMLROOT', 'XMLSERIALIZE', 'XMLUPDATE', 'YEAR', 'NZ_GET_DSID', 'SCAN_DATA_SOURCE_SCHEMA',

        -- SQL Extensions Toolkit AGGREGATES

        'CORR', 'COVAR_POP', 'COVAR_SAMP', 'XMLAGG'
    )

    ----------------------------------------

    AND       upper(\"Database\") NOT IN
    (
        -- These databases are part of INZA.  Let's not check what's in them at this time.

        'INZA', 'NZA', 'NZM', 'NZMSG', 'NZR', 'NZRC', 'NZVERIFY', 'NZPYIDA', 'NZPY'
    )

    ----------------------------------------

    GROUP BY 1,2,3,4
    ORDER by 1,2,3,4
    ;"
)

if [[ -z "$query_output" ]]; then
    echo "There are no third party udx present."
else

    # Creates a file if it doesn't exist. Overwrites any previous content of the file if it exists.
    printf "UDX_LIST" > /nz/export/udx_list.txt
    
    while IFS= read -r line; do
        # Extracting fields from each line using awk
        database=$(echo "$line" | awk '{print $1}')
        schema=$(echo "$line" | awk '{print $3}')
        obj_name=$(echo "$line" | awk '{print $5}')
        obj_type=$(echo "$line" | awk '{print tolower($7)}')

        #Avoid printing for empty lines
        if [[ ! -z "$database" ]]; then

            printf "\n\n------------------------\nThe following ${obj_type} is created against \nDatabase: %s\nSchema: %s\n------------------------\n\n" "${database}" "${schema}" >> /nz/export/udx_list.txt
            /nz/support/bin/nz_ddl_${obj_type} ${database} -schema ${schema} ${obj_name} -udxDir /nz/export/udx_list >> /nz/export/udx_list.txt
        
        fi

    done <<< "$query_output"

    echo "The list of third party udx were successfully saved to /nz/export/udx_list.txt"
fi
