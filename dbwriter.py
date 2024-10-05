import argparse
import csv
import logging

# TODO: Check that fields are valid for record types, count and indicate row #'s
# TODO: Check CSV encoding - read appropriately


def duplicates_found(recname_list):
    """
    Checks for duplicates within a list, and returns the list of duplicates.
    
    params
    ----------------
    recname_list : list
        List of all record names from RECNAME column (with RECTYP and not IGNORE)
        
    returns
    ----------------
    duplicates : set
        Set of RECNAME values that have duplicates
    """   
    duplicates = []
    seen = set()
    for name in recname_list:
        if name in seen:
            duplicates.append(name)
        seen.add(name)
    return set(duplicates)


def input_error(input_path):
    """
    Checks the input filepath for the stats on ignored line and the following errors:
    1. File does not end with .csv - break
    2. Does not contains headers: RECNAME and RECTYPE - break
    3. Rows missing RECNAME or RECTYPE
    4. Duplicate RECNAME instances
    
    params
    ----------------
    input_path : str
        String of full filepath to input CSV file
        
    returns
    ----------------
    err_return : boolean
        Whether the input file contains any formatting errors
    """

    logging.info("Checking input file for errors...")

    err_log = {
        "missing_type": {"msg": "Missing RECTYPE", "rows": []},
        "missing_name": {"msg": "Missing RECNAME", "rows": []},
        "field": {"msg": "Invalid Field Assignment", "rows": []},
        "name": {"msg": "Duplicate Record Names", "rows": []},
        "ignored": {"msg": "Rows ignored", "rows": []},
    }

    logging.debug(f"...opening {input_path}")
    with open(input_path, "r", encoding="utf-8-sig") as csvfile:

        # If input file is not CSV, return error
        logging.debug("...file extension: .csv")
        if not input_path.endswith(".csv"):
            logging.info(f"Error: {input_path} is not a CSV file")
            return True

        reader = csv.DictReader(csvfile)

        # If CSV is missing necessary headers, return error
        headers = reader.fieldnames
        logging.debug("...RECNAME and RECTYPE in headers")
        if (
            headers is None
            or len(headers) < 2
            or "RECNAME" not in headers
            or "RECTYPE" not in headers
        ):
            logging.info("Error: The CSV file must have at least 'RECNAME' and 'RECTYPE'")
            return True


        # Check each row for records with errors
        # If row is missing RECTYPE or RECNAME and not IGNORE, append row to error report
        logging.debug("...rows where IGNORE = true and rows missing RECTYPE or RECNAME")
        recnames = []
        row_num = 1
        for row in reader:
            recname = row["RECNAME"]
            rectype = row["RECTYPE"]

            if "IGNORE" in headers and (row["IGNORE"].lower() == "true"):
                logging.debug(f"......row: {row_num}")
                logging.debug(".........ignored")
                err_log["ignored"]["rows"].append(row_num)
            elif not recname:
                logging.debug(f"......row: {row_num}")
                logging.debug(".........missing RECNAME")
                err_log["missing_name"]["rows"].append(row_num)
            elif not rectype:
                logging.debug(f"......row: {row_num}")
                logging.debug(".........missing RECTYPE")
                err_log["missing_type"]["rows"].append(row_num)
            else:
                recnames.append(recname)
            row_num += 1

        # Check record names for duplicates
        logging.debug("...duplicate RECNAME instances")
        err_log["name"]["rows"] = duplicates_found(recnames)

        # Iterate over err_log dictionary, print any with len(row)>0 && error true
        err_return = False
        err_list = []
        for err in err_log:
            logging.debug(f"{err}: {len(err_log[err]["rows"])}")
            if len(err_log[err]["rows"]) > 0:
                err_list.append(err_log[err]["msg"] + ", rows: " + str(err_log[err]["rows"]))
                err_return = True

        err_message = "\n".join(e for e in err_list)
        if err_return:
            logging.info(err_message)
        else:
            logging.info("No errors found")
            return err_return


def process_csv(input_path, output_db):
    """
    Use CSV indicated by input_path to output an EPICS database file as output_db.

    Input file must have RECTYPE and RECNAME columns. It may have an IGNORE column
    which, if set to "true", will ignore that row/ record. Other column heads will
    be treated as fields of a record and its value set to the corresponding cell.

    params
    ----------------
    input_path : str
        String of full filepath to input CSV file

    output_db : str
        String of full filepath to output '.db' file
        
    returns
    ----------------
    """

    try:
        # Read the CSV file
        logging.debug(f"Opening file: {input_path}")
        with open(input_path, "r", encoding="utf-8-sig") as csvfile:
            reader = csv.DictReader(csvfile)
            headers = reader.fieldnames
            fields = [h for h in headers if h not in ['RECNAME', 'RECTYPE', 'IGNORE']]
            row_num = 0

            # Open the output file
            with open(output_db, "w") as outfile:
                # Process each row in the CSV

                ignore_h = "IGNORE" in headers
                count_recs = 0 # count records written
                count_ignore = 0 # count rows ignored
                count_skip = 0 # count rows skipped

                for row in reader:
                    row_num += 1
                    recname = row["RECNAME"]
                    rectype = row["RECTYPE"]

                    ignore = False
                    skip = False

                    if ignore_h: 
                        ignore = row["IGNORE"].lower() == "true"
                        if ignore:
                            logging.debug(f"...row:{row_num} ignored")
                            count_ignore += 1
                            ignore = True
                    
                    if not (recname and rectype):
                        logging.debug(f"...row:{row_num} skipped - missing name or type")
                        skip = True
                        count_skip += 1

                    if not (ignore or skip): 
                        count_recs +=1

                        # Write the record to the output file
                        outfile.write(f'record ( {rectype}, "{recname}") {{\n')

                        # Write the fields, skipping the empty values
                        for field in fields:
                            value = row.get[field]
                            if value:
                                outfile.write(f'    field ( {field}, "{value}" )\n')

                        outfile.write("}\n\n")
        logging.info(f"{count_recs} records written")
        logging.info(f"{count_ignore} records ignored")
        logging.info(f"{count_skip} records skipped")

    except Exception as e:
        print(f"Error processing the file: {e}")


def main():
    parser = argparse.ArgumentParser(
        description="Generate EPICS databases from a CSV file."
    )
    parser.add_argument(
        "-i", "--input_path", required=True, help="Full path to the input CSV file"
    )
    parser.add_argument(
        "-o",
        "--output_db",
        required=True,
        help="Full path to the output EPICS database file",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        required=False,
        help="Run in verbose mode"
    )

    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(
            format="%(levelname)s: %(asctime)s - %(message)s", level=logging.DEBUG
        )
        logging.info("Verbose output")
    else:
        logging.basicConfig(format="%(levelname)s: %(message)s", level=logging.INFO)

    logging.info(f"Arguments: {args.__dict__}")

    # Process the input CSV and generate the EPICS database
    if not input_error(args.input_path):
        process_csv(args.input_path, args.output_db)


if __name__ == "__main__":
    main()
