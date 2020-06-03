

import java.io.Serializable;

/**
 * This might be useful some time down the line. Maybe.
 */
public class Schema implements Serializable {

  private static final long serialVersionUID = 1L;
    private String API_Well_Number;
    private String County;
    private String Company_Name;
    private String API_Hole_Number;
    private String Sidetrack_Code;
    private String Completion_Code;
    private String Well_Type_Code;
    private String Production_Field;
    private String Well_Status_Code;
    private String Well_Name;
    private String Town;
    private String Producing_Formation;
    private String Months_in_Production;
    private int Gas_Produced;
    private int Water_Produced;
    private int Oil_Produced;
    private String Reporting_Year;
    private String New_Georeferenced_Column;


    
    public String getAPI_Well_Number() {
        return API_Well_Number;
    }

    public void setAPI_Well_Number(String aPI_Well_Number) {
        API_Well_Number = aPI_Well_Number;
    }

    public String getCounty() {
        return County;
    }

    public void setCounty(String county) {
        County = county;
    }

    public String getCompany_Name() {
        return Company_Name;
    }

    public void setCompany_Name(String company_Name) {
        Company_Name = company_Name;
    }

    public String getAPI_Hole_Number() {
        return API_Hole_Number;
    }

    public void setAPI_Hole_Number(String aPI_Hole_Number) {
        API_Hole_Number = aPI_Hole_Number;
    }

    public String getSidetrack_Code() {
        return Sidetrack_Code;
    }

    public void setSidetrack_Code(String sidetrack_Code) {
        Sidetrack_Code = sidetrack_Code;
    }

    public String getCompletion_Code() {
        return Completion_Code;
    }

    public void setCompletion_Code(String completion_Code) {
        Completion_Code = completion_Code;
    }

    public String getWell_Type_Code() {
        return Well_Type_Code;
    }

    public void setWell_Type_Code(String well_Type_Code) {
        Well_Type_Code = well_Type_Code;
    }

    public String getProduction_Field() {
        return Production_Field;
    }

    public void setProduction_Field(String production_Field) {
        Production_Field = production_Field;
    }

    public String getWell_Status_Code() {
        return Well_Status_Code;
    }

    public void setWell_Status_Code(String well_Status_Code) {
        Well_Status_Code = well_Status_Code;
    }

    public String getWell_Name() {
        return Well_Name;
    }

    public void setWell_Name(String well_Name) {
        Well_Name = well_Name;
    }

    public String getTown() {
        return Town;
    }

    public void setTown(String town) {
        Town = town;
    }

    public String getProducing_Formation() {
        return Producing_Formation;
    }

    public void setProducing_Formation(String producing_Formation) {
        Producing_Formation = producing_Formation;
    }

    public String getMonths_in_Production() {
        return Months_in_Production;
    }

    public void setMonths_in_Production(String months_in_Production) {
        Months_in_Production = months_in_Production;
    }

    public int getGas_Produced() {
        return Gas_Produced;
    }

    public void setGas_Produced(int gas_Produced) {
        Gas_Produced = gas_Produced;
    }

    public int getWater_Produced() {
        return Water_Produced;
    }

    public void setWater_Produced(int water_Produced) {
        Water_Produced = water_Produced;
    }

    public int getOil_Produced() {
        return Oil_Produced;
    }

    public void setOil_Produced(int oil_Produced) {
        Oil_Produced = oil_Produced;
    }

    public String getReporting_Year() {
        return Reporting_Year;
    }

    public void setReporting_Year(String reporting_Year) {
        Reporting_Year = reporting_Year;
    }

    public String getNew_Georeferenced_Column() {
        return New_Georeferenced_Column;
    }

    public void setNew_Georeferenced_Column(String new_Georeferenced_Column) {
        New_Georeferenced_Column = new_Georeferenced_Column;
    }

    public Schema(String aPI_Well_Number, String county, String company_Name, String aPI_Hole_Number,
            String sidetrack_Code, String completion_Code, String well_Type_Code, String production_Field,
            String well_Status_Code, String well_Name, String town, String producing_Formation,
            String months_in_Production, int gas_Produced, int water_Produced, int oil_Produced, String reporting_Year,
            String new_Georeferenced_Column) {
        API_Well_Number = aPI_Well_Number;
        County = county;
        Company_Name = company_Name;
        API_Hole_Number = aPI_Hole_Number;
        Sidetrack_Code = sidetrack_Code;
        Completion_Code = completion_Code;
        Well_Type_Code = well_Type_Code;
        Production_Field = production_Field;
        Well_Status_Code = well_Status_Code;
        Well_Name = well_Name;
        Town = town;
        Producing_Formation = producing_Formation;
        Months_in_Production = months_in_Production;
        Gas_Produced = gas_Produced;
        Water_Produced = water_Produced;
        Oil_Produced = oil_Produced;
        Reporting_Year = reporting_Year;
        New_Georeferenced_Column = new_Georeferenced_Column;
    }


}