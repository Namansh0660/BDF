package com.hp;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

public class ExcelDataCleaner {
    public static void main(String[] args) {
        String sourceFile = "/Users/HP/Documents/bigdataframeworks/Table 2.xlsx";
        String destinationFile = "/Users/HP/Documents/bigdataframeworks/Cleaned_Table2.xlsx";

        try (FileInputStream fileIn = new FileInputStream(sourceFile);
             Workbook originalWorkbook = new XSSFWorkbook(fileIn);
             Workbook processedWorkbook = new XSSFWorkbook()) {

            Sheet originalSheet = originalWorkbook.getSheetAt(0);
            Sheet processedSheet = processedWorkbook.createSheet(originalSheet.getSheetName());

            int processedRowCount = 0;

            for (Row currentRow : originalSheet) {
                Row newRow = processedSheet.createRow(processedRowCount);
                boolean isRowValid = true;
                List<Object> processedData = new ArrayList<>();

                for (Cell currentCell : currentRow) {
                    Cell newCell = newRow.createCell(currentCell.getColumnIndex());
                    switch (currentCell.getCellType()) {
                        case STRING:
                            String cellValue = currentCell.getStringCellValue().trim();
                            if (cellValue.isEmpty()) {
                                isRowValid = false;
                            }
                            newCell.setCellValue(cellValue);
                            processedData.add(cellValue);
                            break;

                        case NUMERIC:
                            newCell.setCellValue(currentCell.getNumericCellValue());
                            processedData.add(currentCell.getNumericCellValue());
                            break;

                        case BOOLEAN:
                            newCell.setCellValue(currentCell.getBooleanCellValue());
                            processedData.add(currentCell.getBooleanCellValue());
                            break;

                        default:
                            newCell.setCellValue("N/A");
                            isRowValid = false;
                            processedData.add("N/A");
                    }
                }

                if (processedData.contains(null) || processedData.contains("")) {
                    isRowValid = false;
                }

                if (isRowValid) {
                    processedRowCount++;
                } else {
                    processedSheet.removeRow(newRow);
                }
            }

            removeDuplicateRows(processedSheet);

            try (FileOutputStream fileOut = new FileOutputStream(destinationFile)) {
                processedWorkbook.write(fileOut);
            }

            System.out.println("Data cleaning completed. Cleaned data saved to " + destinationFile);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void removeDuplicateRows(Sheet sheet) {
        Set<String> uniqueRowContents = new HashSet<>();
        List<Integer> rowsForDeletion = new ArrayList<>();

        for (Row currentRow : sheet) {
            StringBuilder rowContent = new StringBuilder();

            for (Cell cell : currentRow) {
                if (cell.getCellType() == CellType.STRING) {
                    rowContent.append(cell.getStringCellValue()).append("|");
                } else if (cell.getCellType() == CellType.NUMERIC) {
                    rowContent.append(cell.getNumericCellValue()).append("|");
                }
            }

            if (!uniqueRowContents.add(rowContent.toString())) {
                rowsForDeletion.add(currentRow.getRowNum());
            }
        }

        for (int i = rowsForDeletion.size() - 1; i >= 0; i--) {
            int rowIndex = rowsForDeletion.get(i);
            sheet.removeRow(sheet.getRow(rowIndex));
            sheet.shiftRows(rowIndex + 1, sheet.getLastRowNum(), -1);
        }
    }
}
