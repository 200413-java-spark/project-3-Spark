package com.github.p3spark;


import java.io.Serializable;

/**
 * This might be useful some time down the line. Maybe.
 * All of the fields are here, but the data type may not be correct
 */
public class Schema implements Serializable {

  private static final long serialVersionUID = 1L;
    private String APIWellNumber;
    private String County;
    private String CompanyName;
    private String APIHoleNumber;
    private String SidetrackCode;
    private String CompletionCode;
    private String WellTypeCode;
    private String ProductionField;
    private String WellStatusCode;
    private String WellName;
    private String Town;
    private String ProducingFormation;
    private String MonthsinProduction;
    private int GasProduced;
    private int WaterProduced;
    private int OilProduced;
    private String ReportingYear;
    private String NewGeoreferencedColumn;

    
    public String getAPIWellNumber() {
        return APIWellNumber;
    }

    public void setAPIWellNumber(String aPIWellNumber) {
        APIWellNumber = aPIWellNumber;
    }

    public String getCounty() {
        return County;
    }

    public void setCounty(String county) {
        County = county;
    }

    public String getCompanyName() {
        return CompanyName;
    }

    public void setCompanyName(String companyName) {
        CompanyName = companyName;
    }

    public String getAPIHoleNumber() {
        return APIHoleNumber;
    }

    public void setAPIHoleNumber(String aPIHoleNumber) {
        APIHoleNumber = aPIHoleNumber;
    }

    public String getSidetrackCode() {
        return SidetrackCode;
    }

    public void setSidetrackCode(String sidetrackCode) {
        SidetrackCode = sidetrackCode;
    }

    public String getCompletionCode() {
        return CompletionCode;
    }

    public void setCompletionCode(String completionCode) {
        CompletionCode = completionCode;
    }

    public String getWellTypeCode() {
        return WellTypeCode;
    }

    public void setWellTypeCode(String wellTypeCode) {
        WellTypeCode = wellTypeCode;
    }

    public String getProductionField() {
        return ProductionField;
    }

    public void setProductionField(String productionField) {
        ProductionField = productionField;
    }

    public String getWellStatusCode() {
        return WellStatusCode;
    }

    public void setWellStatusCode(String wellStatusCode) {
        WellStatusCode = wellStatusCode;
    }

    public String getWellName() {
        return WellName;
    }

    public void setWellName(String wellName) {
        WellName = wellName;
    }

    public String getTown() {
        return Town;
    }

    public void setTown(String town) {
        Town = town;
    }

    public String getProducingFormation() {
        return ProducingFormation;
    }

    public void setProducingFormation(String producingFormation) {
        ProducingFormation = producingFormation;
    }

    public String getMonthsinProduction() {
        return MonthsinProduction;
    }

    public void setMonthsinProduction(String monthsinProduction) {
        MonthsinProduction = monthsinProduction;
    }

    public int getGasProduced() {
        return GasProduced;
    }

    public void setGasProduced(int gasProduced) {
        GasProduced = gasProduced;
    }

    public int getWaterProduced() {
        return WaterProduced;
    }

    public void setWaterProduced(int waterProduced) {
        WaterProduced = waterProduced;
    }

    public int getOilProduced() {
        return OilProduced;
    }

    public void setOilProduced(int oilProduced) {
        OilProduced = oilProduced;
    }

    public String getReportingYear() {
        return ReportingYear;
    }

    public void setReportingYear(String reportingYear) {
        ReportingYear = reportingYear;
    }

    public String getNewGeoreferencedColumn() {
        return NewGeoreferencedColumn;
    }

    public void setNewGeoreferencedColumn(String newGeoreferencedColumn) {
        NewGeoreferencedColumn = newGeoreferencedColumn;
    }

    public Schema(String aPIWellNumber, String county, String companyName, String aPIHoleNumber,
            String sidetrackCode, String completionCode, String wellTypeCode, String productionField,
            String wellStatusCode, String wellName, String town, String producingFormation,
            String monthsinProduction, int gasProduced, int waterProduced, int oilProduced, String reportingYear,
            String newGeoreferencedColumn) {
        APIWellNumber = aPIWellNumber;
        County = county;
        CompanyName = companyName;
        APIHoleNumber = aPIHoleNumber;
        SidetrackCode = sidetrackCode;
        CompletionCode = completionCode;
        WellTypeCode = wellTypeCode;
        ProductionField = productionField;
        WellStatusCode = wellStatusCode;
        WellName = wellName;
        Town = town;
        ProducingFormation = producingFormation;
        MonthsinProduction = monthsinProduction;
        GasProduced = gasProduced;
        WaterProduced = waterProduced;
        OilProduced = oilProduced;
        ReportingYear = reportingYear;
        NewGeoreferencedColumn = newGeoreferencedColumn;
    }


}