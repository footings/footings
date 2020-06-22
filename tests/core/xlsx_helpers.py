# This file was copyied from XlsxWriter/test/helperfunctions.py
# b644cf25c0f252735a99faa6af3675dbf8216801
#
# Edits to the file include:
# - Commenting out line 167
# - Added logging
# - Added sort attributes function within _sort_rel_file_data
# - Apply _sort_rel_file_data to all files

# pylint: disable-all

###############################################################################
#
# Helper functions for testing XlsxWriter.
#
# Copyright (c), 2013-2020, John McNamara, jmcnamara@cpan.org
#
# LICENSE
# Copyright (c) 2013-2020, John McNamara <jmcnamara@cpan.org>
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice, this
#    list of conditions and the following disclaimer.
# 2. Redistributions in binary form must reproduce the above copyright notice,
#    this list of conditions and the following disclaimer in the documentation
#    and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
# ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#
# The views and conclusions contained in the software and documentation are those
# of the authors and should not be interpreted as representing official policies,
# either expressed or implied, of the FreeBSD Project.

import re
import sys
import os.path
from zipfile import ZipFile
from zipfile import BadZipfile
from zipfile import LargeZipFile
import logging
import xml.etree.ElementTree as ET

LOGGER = logging.getLogger(__name__)


def _xml_to_list(xml_str):
    # Convert test generated XML strings into lists for comparison testing.

    # Split the XML string at tag boundaries.
    parser = re.compile(r">\s*<")
    elements = parser.split(xml_str.strip())

    elements = [s.replace("\r", "") for s in elements]

    # Add back the removed brackets.
    for index, element in enumerate(elements):
        if not element[0] == "<":
            elements[index] = "<" + elements[index]
        if not element[-1] == ">":
            elements[index] = elements[index] + ">"

    return elements


def _vml_to_list(vml_str):
    # Convert an Excel generated VML string into a list for comparison testing.
    #
    # The VML data in the testcases is taken from Excel 2007 files. The data
    # has to be massaged significantly to make it suitable for comparison.
    #
    # The VML produced by XlsxWriter can be parsed as ordinary XML.
    vml_str = vml_str.replace("\r", "")

    vml = vml_str.split("\n")
    vml_str = ""

    for line in vml:
        # Skip blank lines.
        if not line:
            continue

        # Strip leading and trailing whitespace.
        line = line.strip()

        # Convert VMLs attribute quotes.
        line = line.replace("'", '"')

        # Add space between attributes.
        if re.search('"$', line):
            line += " "

        # Add newline after element end.
        if re.search(">$", line):
            line += "\n"

        # Split multiple elements.
        line = line.replace("><", ">\n<")

        # Put all of Anchor on one line.
        if line == "<x:Anchor>\n":
            line = line.strip()

        vml_str += line

    # Remove the final newline.
    vml_str = vml_str.rstrip()

    return vml_str.split("\n")


def _sort_rel_file_data(xml_elements):
    # Re-order the relationship elements in an array of XLSX XML rel
    # (relationship) data. This is necessary for comparison since
    # Excel can produce the elements in a semi-random order.

    # We don't want to sort the first or last elements.
    first = xml_elements.pop(0)
    last = xml_elements.pop()

    # Sort the relationship elements.
    def sort_attributes(xml):
        try:
            root = ET.fromstring(xml)
            root.attrib = {k: root.attrib[k] for k in sorted(root.attrib.keys())}
            xmlstr = ET.tostring(root, encoding="utf-8", method="xml")
            ret = xmlstr.decode("utf-8")
        except ET.ParseError:
            ret = xml
        return ret

    xml_elements = sorted([sort_attributes(xml) for xml in xml_elements])

    # Add back the first and last elements.
    xml_elements.insert(0, first)
    xml_elements.append(last)

    return xml_elements


def _compare_xlsx_files(got_file, exp_file, ignore_files, ignore_elements):
    # Compare two XLSX files by extracting the XML files from each
    # zip archive and comparing them.
    #
    # This is used to compare an "expected" file produced by Excel
    # with a "got" file produced by XlsxWriter.
    #
    # In order to compare the XLSX files we convert the data in each
    # XML file into an list of XML elements.

    LOGGER.info("Starting xlsx comparison...")

    try:
        # Open the XlsxWriter as a zip file for testing.
        got_zip = ZipFile(got_file, "r")
    except IOError as e:
        error = "XlsxWriter file error: " + str(e)
        return error, ""
    except (BadZipfile, LargeZipFile) as e:
        error = "XlsxWriter zipfile error, '" + exp_file + "': " + str(e)
        return error, ""

    try:
        # Open the Excel as a zip file for testing.
        exp_zip = ZipFile(exp_file, "r")
    except IOError as e:
        error = "Excel file error: " + str(e)
        return error, ""
    except (BadZipfile, LargeZipFile) as e:
        error = "Excel zipfile error, '" + exp_file + "': " + str(e)
        return error, ""

    # Get the filenames from the zip files.
    got_files = sorted(got_zip.namelist())
    exp_files = sorted(exp_zip.namelist())

    # Ignore some test specific filenames.
    got_files = [name for name in got_files if name not in ignore_files]
    exp_files = [name for name in exp_files if name not in ignore_files]

    # Check that each XLSX container has the same files.
    if got_files != exp_files:
        return got_files, exp_files

    LOGGER.info("XLSX passed same files...")

    # Compare each file in the XLSX containers.
    for filename in exp_files:

        LOGGER.info(f"Starting comparison of {filename}...")

        got_xml_str = got_zip.read(filename)
        exp_xml_str = exp_zip.read(filename)

        # Compare binary files with string comparison based on extension.
        extension = os.path.splitext(filename)[1]
        if extension in (".png", ".jpeg", ".bmp", ".wmf", ".emf", ".bin"):
            if got_xml_str != exp_xml_str:
                return "got: %s" % filename, "exp: %s" % filename
            continue

        if sys.version_info >= (3, 0, 0):
            got_xml_str = got_xml_str.decode("utf-8")
            exp_xml_str = exp_xml_str.decode("utf-8")

        # Remove dates and user specific data from the core.xml data.
        if filename == "docProps/core.xml":
            # exp_xml_str = re.sub(r' ?John', '', exp_xml_str)
            exp_xml_str = re.sub(r"\d\d\d\d-\d\d-\d\dT\d\d\:\d\d:\d\dZ", "", exp_xml_str)
            got_xml_str = re.sub(r"\d\d\d\d-\d\d-\d\dT\d\d\:\d\d:\d\dZ", "", got_xml_str)

        # Remove workbookView dimensions which are almost always different
        # and calcPr which can have different Excel version ids.
        if filename == "xl/workbook.xml":
            exp_xml_str = re.sub(r"<workbookView[^>]*>", "<workbookView/>", exp_xml_str)
            got_xml_str = re.sub(r"<workbookView[^>]*>", "<workbookView/>", got_xml_str)
            exp_xml_str = re.sub(r"<calcPr[^>]*>", "<calcPr/>", exp_xml_str)
            got_xml_str = re.sub(r"<calcPr[^>]*>", "<calcPr/>", got_xml_str)

        # Remove printer specific settings from Worksheet pageSetup elements.
        if re.match(r"xl/worksheets/sheet\d.xml", filename):
            exp_xml_str = re.sub(r'horizontalDpi="200" ', "", exp_xml_str)
            exp_xml_str = re.sub(r'verticalDpi="200" ', "", exp_xml_str)
            exp_xml_str = re.sub(r'(<pageSetup[^>]*) r:id="rId1"', r"\1", exp_xml_str)

        # Remove Chart pageMargin dimensions which are almost always different.
        if re.match(r"xl/charts/chart\d.xml", filename):
            exp_xml_str = re.sub(r"<c:pageMargins[^>]*>", "<c:pageMargins/>", exp_xml_str)
            got_xml_str = re.sub(r"<c:pageMargins[^>]*>", "<c:pageMargins/>", got_xml_str)

        # Convert the XML string to lists for comparison.
        if re.search(".vml$", filename):
            got_xml = _xml_to_list(got_xml_str)
            exp_xml = _vml_to_list(exp_xml_str)
        else:
            got_xml = _xml_to_list(got_xml_str)
            exp_xml = _xml_to_list(exp_xml_str)

        # Ignore test specific XML elements for defined filenames.
        if filename in ignore_elements:
            patterns = ignore_elements[filename]
            print(patterns)

            for pattern in patterns:
                exp_xml = [tag for tag in exp_xml if not re.match(pattern, tag)]
                got_xml = [tag for tag in got_xml if not re.match(pattern, tag)]

        # Reorder the XML elements in the XLSX relationship files.
        got_xml = _sort_rel_file_data(got_xml)
        exp_xml = _sort_rel_file_data(exp_xml)

        # Compared the XML elements in each file.
        if got_xml != exp_xml:
            got_xml.insert(0, filename)
            exp_xml.insert(0, filename)

            LOGGER.info(f"XML different for file {filename}...")
            LOGGER.info(f"Got XML = {str(got_xml)}")
            LOGGER.info(f"Expected XML = {str(exp_xml)}")
            return got_xml, exp_xml

        LOGGER.info(f"Comparison of {filename} passed")

    # If we got here the files are the same.
    return "Ok", "Ok"


# External wrapper function to allow simplified equality testing of two Excel
# files. Note, this function doesn't test equivalence, only equality.
def compare_xlsx_files(file1, file2, ignore_files=None, ignore_elements=None):

    if ignore_files is None:
        ignore_files = []

    if ignore_elements is None:
        ignore_elements = []

    got, exp = _compare_xlsx_files(file1, file2, ignore_files, ignore_elements)

    return got == exp
