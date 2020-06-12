CREATE TABLE county_vs_oil_production_by_year(
        id serial primary key,
        county varchar ,
        oil_production decimal,
        gas_production decimal,
        water_production decimal,
        year int
);

