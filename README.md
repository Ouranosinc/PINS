# PINS

Portraits d'indices de neige au sol / Bias-adjusted projections of snow cover over the Quebec Province using an ensemble of regional climate models.

This repository stores the code that generates the PINS datasets of daily snow water equivalent projections and the related indices. The PINS project was first described in Bresson et al. (2024) (Ouranos project report) and is fully analyzed in Bresson et al. (2025) (journal article). This page is only a short summary describing the code and the data.

![](resources/pins.png)

## Context
In the context of climate change, stakeholders and decision-makers are in demand of easily accessible bias-adjusted projections of snow cover and their resulting indices to develop adaptation plans. To meet this need, we produced an ensemble of regional climate projections statistically bias adjusted of snow water equivalent (SWE) over the Quebec province. This bias adjustment required some fine-tuning to operational methods, mainly due to the seasonality in the SWE. We calculated SWE indices of interest for several sectors based on the bias-adjusted SWE. These indices include, but are not limited, the maximum of SWE, duration, start, and end of the snow season.

## Dataset composition

Table 1 below shows the members of PINS, the [CORDEX-NA CMIP5 RCMs](https://na-cordex.org/) that were selected through an initial screening. The selection was based upon the RCMs ability to simulate the start and end of the snow season when compared to ERA5-Land. 

**Table 1. Members of PINS v1.0**

| **Institution** | **Model** | **Experiment** | **Driving model** | **Resolution** |
|-----------------|-----------|----------------|-------------------|----------------|
| UQAM            | CRCM5     | RCP4.5         | MPI-ESM-LR        | 0.44°          |
| OURANOS         | CRCM5     | RCP4.5         | CNRM-CM5          | 0.22°          |
| OURANOS         | CRCM5     | RCP4.5         | GFDL-ESM2M        | 0.22°          |
| OURANOS         | CRCM5     | RCP4.5         | MPI-ESM-LR        | 0.22°          |
| Iowa State Uni. | RegCM4    | RCP8.5         | HadGEM2-ES        | 0.22°          |
| UCAR            | RegCM4    | RCP8.4         | MPI-ESM-LR        | 0.22°          |
| NCAR            | WRF       | RCP8.5         | GFDL-ESM2M        | 0.22°          |
| OURANOS         | CRCM5     | RCP8.5         | CNRM-CM5          | 0.22°          |
| OURANOS         | CRCM5     | RCP8.5         | GFDL-ESM"M        | 0.22°          |
| OURANOS         | CRCM5     | RCP8.5         | MPI-ESM-LR        | 0.22°          |

The PINS project that produced this dataset aimed to deliver a collection of annual surface snow indices. A selection of the most robust indices is shared alongside this repository on Zenodo.

The first step, however, was to produce bias-adjusted timeseries of surface snow amount (`snw`, also called "snow water equivalent", SWE).

## Spatial and temporal coverage
The PINS data was produced over a region slightly larger than Québec, over northeastern North America. **TODO: Les bounds. Une carte ?**

Daily snow amount data was produced for the whole length of the RCM simulations, so 1950 to 2100 (or 2099 in some cases). Indicators were computed on each year of the period. The reference period for the adjustment was chosen to be 1981-2010.

## Reference data
The bias-adjustment was made in reference to ERA5-Land (Muñoz-Sabater et al. 2021) data which was selected out of 4 candidate products (Blended-5, MERRA-2, ERA5 and ERA5-Land) by testing its performance against station observations from the CanSWE dataset (Vionnet et al., 2021). This followed the conclusions of Mudryk et al. (2024).

## Methodology
The snow amount presents a strong seasonality which makes conventional bias-adjustment methods harder to apply. Source simulations that did not simulate the beginning and end of the snow season appropriately were rejected from the ensemble. Following Michel et al. (2023), the end of the season was slowed down using an exponential decay as a preliminary step, in order to let the quantile-mapping adjustment do its job.

## Data processing tools
The code relies on [xscen](https://xscen.readthedocs.io/) for the workflow management, [xsdba](https://xsdba.readthedocs.io/) for the bias-adjustment and [xclim](https://xclim.readthedocs.io/) for the indicators calculations, three open-source python libraries maintained by Ouranos. All are built upon the packages xarray and dask for data handling and parallelization management.

**TODO : Description sommaire du code, au minimum pour montrer ce qu'il manque à qqn qui voudrait le rouler.**

## Performance

## Data availability and download
A selection of indicators is made available directly in the Zenodo repository linked to the code repository.

The daily timeseries are available on Ouranos' PAVICS THREDDS server :
- **TODO : Les urls**

## References

Bresson, É., Dupuis, É. et Bourgault, P. (2024). PINS - Portrait des indices de neige au sol. Rapport présenté à l'Association des Stations de ski du Québec et au gouvernement du Québec. Ouranos, Montréal, Canada. 34 pages + Annexes 9 pages. [URL](https://www.ouranos.ca/sites/default/files/2024-11/proj-202025-eco-scsc-pins-bresson-rapportfinal.pdf)

Bresson, É., Dupuis, É. & Bourgault, P. (2025). Bias-adjusted projections of snow cover over the Quebec Province using an ensemble of regional climate models (in writing)

Mearns, L.O., et al., 2017: The NA-CORDEX dataset, version 1.0. NCAR Climate Data Gateway, Boulder CO, https://doi.org/10.5065/D6SJ1JCH

Michel, A., Aschauer, J., Jonas, T., Gubler, S., Kotlarski, S., & Marty, C. (2023). SnowQM 1.0: A fast R Package for bias-correcting spatial fields of snow water equivalent using quantile mapping. Geosci. Model Dev. Discuss., 2023, 1–28. https://doi.org/10.5194/gmd-2022-298

Mudryk, L. R., Mortimer, C., Derksen, C., Elias Chereque, A., & Kushner, P. J. (2024). Benchmarking of SWE products based on outcomes of the SnowPEx+ Intercomparison Project. EGUsphere, 2024, 1–28. https://doi.org/10.5194/egusphere-2023-3014

Muñoz-Sabater, J., Dutra, E., Agustí-Panareda, A., Albergel, C., Arduini, G., Balsamo, G., Boussetta, S., Choulga, M., Harrigan, S., Hersbach, H., Martens, B., Miralles, D. G., Piles, M., Rodríguez-Fernández, N. J., Zsoter, E., Buontempo, C., & Thépaut, J.-N. (2021). ERA5-Land: a state-of-the-art global reanalysis dataset for land applications. Earth Syst. Sci. Data, 13(9),  4349–4383. https://doi.org/10.5194/essd-13-4349-2021

Vionnet, V., Mortimer, C., Brady, M., Arnal, L., & Brown, R. (2021). Canadian historical Snow Water Equivalent dataset (CanSWE, 1928–2020). Earth Syst. Sci. Data, 13(9), 4603–4619. https://doi.org/10.5194/essd-13-4603-2021
