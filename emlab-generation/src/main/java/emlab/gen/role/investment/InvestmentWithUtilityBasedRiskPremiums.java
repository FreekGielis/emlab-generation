/*******************************************************************************
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package emlab.gen.role.investment;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.math.stat.regression.SimpleRegression;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.data.annotation.Transient;
import org.springframework.data.neo4j.annotation.NodeEntity;
import org.springframework.data.neo4j.aspects.core.NodeBacked;
import org.springframework.data.neo4j.support.Neo4jTemplate;
import org.springframework.transaction.annotation.Transactional;

import agentspring.role.Role;
import emlab.gen.domain.agent.BigBank;
import emlab.gen.domain.agent.DecarbonizationModel;
import emlab.gen.domain.agent.EnergyProducer;
import emlab.gen.domain.agent.Government;
import emlab.gen.domain.agent.PowerPlantManufacturer;
import emlab.gen.domain.agent.StochasticTargetInvestor;
import emlab.gen.domain.agent.StrategicReserveOperator;
import emlab.gen.domain.agent.TargetInvestor;
import emlab.gen.domain.contract.CashFlow;
import emlab.gen.domain.contract.Loan;
import emlab.gen.domain.gis.Zone;
import emlab.gen.domain.market.CO2Auction;
import emlab.gen.domain.market.ClearingPoint;
import emlab.gen.domain.market.electricity.ElectricitySpotMarket;
import emlab.gen.domain.market.electricity.Segment;
import emlab.gen.domain.market.electricity.SegmentLoad;
import emlab.gen.domain.policy.PowerGeneratingTechnologyTarget;
import emlab.gen.domain.technology.PowerGeneratingTechnology;
import emlab.gen.domain.technology.PowerGeneratingTechnologyNodeLimit;
import emlab.gen.domain.technology.PowerGridNode;
import emlab.gen.domain.technology.PowerPlant;
import emlab.gen.domain.technology.Substance;
import emlab.gen.domain.technology.SubstanceShareInFuelMix;
import emlab.gen.repository.Reps;
import emlab.gen.repository.StrategicReserveOperatorRepository;
import emlab.gen.util.GeometricTrendRegression;
import emlab.gen.util.MapValueComparator;

/**
 * Creates investment options, calculates risk premiums, evaluates the options,
 * invests
 *
 * @author FreekGielis
 *
 */

@Configurable
@NodeEntity
public class InvestmentWithUtilityBasedRiskPremiums<T extends EnergyProducer> extends GenericInvestmentRole<T>
implements Role<T>, NodeBacked {

    @Transient
    @Autowired
    Reps reps;

    @Transient
    @Autowired
    Neo4jTemplate template;

    @Transient
    @Autowired
    StrategicReserveOperatorRepository strategicReserveOperatorRepository;

    // ===================== Market expectations ====================
    @Transient
    Map<ElectricitySpotMarket, MarketInformation> marketInfoMap = new HashMap<ElectricitySpotMarket, MarketInformation>();

    // -------------------- Start: for each agent ------------------

    @Override
    public void act(T agent) {


        long futureTimePoint = getCurrentTick() + agent.getInvestmentFutureTimeHorizon();
        // logger.warn(agent + " is looking at timepoint " + futureTimePoint);

        // Map expected fuel prices

        Map<Substance, Double> expectedFuelPrices = predictFuelPrices(agent, futureTimePoint);

        // CO2
        Map<ElectricitySpotMarket, Double> expectedCO2Price = determineExpectedCO2PriceInclTaxAndFundamentalForecast(
                futureTimePoint, agent.getNumberOfYearsBacklookingForForecasting(), 0, getCurrentTick());

        // logger.warn("{} expects CO2 prices {}", agent.getName(),
        // expectedCO2Price);

        Map<ElectricitySpotMarket, Double> expectedCO2PriceOld = determineExpectedCO2PriceInclTax(futureTimePoint,
                agent.getNumberOfYearsBacklookingForForecasting(), getCurrentTick());
        // logger.warn("{} used to expect CO2 prices {}", agent.getName(),
        // expectedCO2PriceOld);

        // logger.warn(expectedCO2Price.toString());

        // Demand
        Map<ElectricitySpotMarket, Double> expectedDemand = new HashMap<ElectricitySpotMarket, Double>();
        for (ElectricitySpotMarket elm : reps.template.findAll(ElectricitySpotMarket.class)) {
            GeometricTrendRegression gtr = new GeometricTrendRegression();
            for (long time = getCurrentTick(); time > getCurrentTick()
                    - agent.getNumberOfYearsBacklookingForForecasting()
                    && time >= 0; time = time - 1) {
                gtr.addData(time, elm.getDemandGrowthTrend().getValue(time));
            }
            expectedDemand.put(elm, gtr.predict(futureTimePoint));

            // logger.warn("time," + getCurrentTick() + ",expDemandGrowth," +
            // expectedDemand);
        }

        // ======================= Investment decision ====================

        // for (ElectricitySpotMarket market :
        // reps.genericRepository.findAllAtRandom(ElectricitySpotMarket.class))
        // {
        ElectricitySpotMarket market = agent.getInvestorMarket();
        MarketInformation marketInformation = new MarketInformation(market, expectedDemand, expectedFuelPrices,
                expectedCO2Price.get(market).doubleValue(), futureTimePoint);
        /*
         * if (marketInfoMap.containsKey(market) &&
         * marketInfoMap.get(market).time == futureTimePoint) {
         * marketInformation = marketInfoMap.get(market); } else {
         * marketInformation = new MarketInformation(market, expectedFuelPrices,
         * expectedCO2Price, futureTimePoint); marketInfoMap.put(market,
         * marketInformation); }
         */

        // logger.warn(agent + " is expecting a CO2 price of " +
        // expectedCO2Price.get(market) + " Euro/MWh at timepoint "
        // + futureTimePoint + " in Market " + market);

        // logger.warn("Agent {}  found the expected prices to be {}", agent,
        // marketInformation.expectedElectricityPricesPerSegment);

        // logger.warn("Agent {}  found that the installed capacity in the market {} in future to be "
        // + marketInformation.capacitySum +
        // "and expectde maximum demand to be "
        // + marketInformation.maxExpectedLoad, agent, market);

        /*
         * Here the highest values are initialized to enable evaluation of all
         * investment options
         */

        double highestValue = Double.MIN_VALUE;
        double highestValueWithoutRiskPremium = Double.MIN_VALUE;
        PowerGeneratingTechnology bestTechnology = null;
        PowerGeneratingTechnology bestTechnologyWithoutRiskPremium = null;
        PowerGridNode bestNode = null;
        PowerGridNode bestNodeWithoutRiskPremium = null;

        for (PowerGeneratingTechnology technology : reps.genericRepository.findAll(PowerGeneratingTechnology.class)) {

            DecarbonizationModel model = reps.genericRepository.findAll(DecarbonizationModel.class).iterator().next();

            if (technology.isIntermittent() && model.isNoPrivateIntermittentRESInvestment())
                continue;

            Iterable<PowerGridNode> possibleInstallationNodes;

            /*
             * For dispatchable technologies just choose a random node. For
             * intermittent evaluate all possibilities.
             */
            if (technology.isIntermittent())
                possibleInstallationNodes = reps.powerGridNodeRepository.findAllPowerGridNodesByZone(market.getZone());
            else {
                possibleInstallationNodes = new LinkedList<PowerGridNode>();
                ((LinkedList<PowerGridNode>) possibleInstallationNodes).add(reps.powerGridNodeRepository
                        .findAllPowerGridNodesByZone(market.getZone()).iterator().next());
            }

            // logger.warn("Calculating for " + technology.getName() +
            // ", for Nodes: "
            // + possibleInstallationNodes.toString());

            for (PowerGridNode node : possibleInstallationNodes) {

                // Create potential powerplant to invest in
                PowerPlant plant = new PowerPlant();
                plant.specifyNotPersist(getCurrentTick(), agent, node, technology);

                // if too much capacity of this technology in the pipeline (not
                // limited to the 5 years)

                double expectedInstalledCapacityOfTechnology = reps.powerPlantRepository
                        .calculateCapacityOfExpectedOperationalPowerPlantsInMarketAndTechnology(market, technology,
                                futureTimePoint);
                PowerGeneratingTechnologyTarget technologyTarget = reps.powerGenerationTechnologyTargetRepository
                        .findOneByTechnologyAndMarket(technology, market);

                // More than technology target in the pipeline?

                if (technologyTarget != null) {
                    double technologyTargetCapacity = technologyTarget.getTrend().getValue(futureTimePoint);
                    expectedInstalledCapacityOfTechnology = (technologyTargetCapacity > expectedInstalledCapacityOfTechnology) ? technologyTargetCapacity
                            : expectedInstalledCapacityOfTechnology;
                }
                double pgtNodeLimit = Double.MAX_VALUE;
                PowerGeneratingTechnologyNodeLimit pgtLimit = reps.powerGeneratingTechnologyNodeLimitRepository
                        .findOneByTechnologyAndNode(technology, plant.getLocation());
                if (pgtLimit != null) {
                    pgtNodeLimit = pgtLimit.getUpperCapacityLimit(futureTimePoint);
                }

                // Create expected capacity of technology in node

                double expectedInstalledCapacityOfTechnologyInNode = reps.powerPlantRepository
                        .calculateCapacityOfExpectedOperationalPowerPlantsByNodeAndTechnology(plant.getLocation(),
                                technology, futureTimePoint);

                // Create expected capacity owned by agent in the market

                double expectedOwnedTotalCapacityInMarket = reps.powerPlantRepository
                        .calculateCapacityOfExpectedOperationalPowerPlantsInMarketByOwner(market, futureTimePoint,
                                agent);

                // Create expected capacity owned of this technology

                double expectedOwnedCapacityInMarketOfThisTechnology = reps.powerPlantRepository
                        .calculateCapacityOfExpectedOperationalPowerPlantsInMarketByOwnerAndTechnology(market,
                                technology, futureTimePoint, agent);

                // Create capacity of technology in pipeline

                double capacityOfTechnologyInPipeline = reps.powerPlantRepository
                        .calculateCapacityOfPowerPlantsByTechnologyInPipeline(technology, getCurrentTick());

                // Create current operational capacity of technology

                double operationalCapacityOfTechnology = reps.powerPlantRepository
                        .calculateCapacityOfOperationalPowerPlantsByTechnology(technology, getCurrentTick());

                // Create total capacity in pipeline in the market

                double capacityInPipelineInMarket = reps.powerPlantRepository
                        .calculateCapacityOfPowerPlantsByMarketInPipeline(market, getCurrentTick());

                // If the maximum fraction of the technology with respect to the
                // max load
                // is expected to be exceeded when new plant is added, do not
                // invest

                if ((expectedInstalledCapacityOfTechnology + plant.getActualNominalCapacity())
                        / (marketInformation.maxExpectedLoad + plant.getActualNominalCapacity()) > technology
                        .getMaximumInstalledCapacityFractionInCountry()) {
                    // logger.warn(agent
                    // +
                    // " will not invest in {} technology because there's too much of this type in the market",
                    // technology);

                    // if expected installed cap with new plant exceeds node
                    // limit

                } else if ((expectedInstalledCapacityOfTechnologyInNode + plant.getActualNominalCapacity()) > pgtNodeLimit) {
                    // logger.warn("Technology {} node limit reached",
                    // technology);
                    // If the cap owned of this technology by agent exceeds the
                    // max allowed fraction per agent

                } else if (expectedOwnedCapacityInMarketOfThisTechnology > expectedOwnedTotalCapacityInMarket
                        * technology.getMaximumInstalledCapacityFractionPerAgent()) {
                    // logger.warn(agent
                    // +
                    // " will not invest in {} technology because there's too much capacity planned by him",
                    // technology);

                    // If cap in pipeline exceeds 20% of the max expected load

                } else if (capacityInPipelineInMarket > 0.2 * marketInformation.maxExpectedLoad) {
                    // logger.warn("Not investing because more than 20% of demand in pipeline.");

                    // If cap of technology in pipeline is higher than 2 times
                    // the existing
                    // capacity of technology and more than 9000 MW

                } else if ((capacityOfTechnologyInPipeline > 2.0 * operationalCapacityOfTechnology)
                        && capacityOfTechnologyInPipeline > 9000) { // TODO:
                    // reflects that you cannot expand a technology out of zero.
                    // logger.warn(agent
                    // +
                    // " will not invest in {} technology because there's too much capacity in the pipeline",
                    // technology);

                    // If agent is not able to pay the equity fraction of the
                    // investment (downpayment)
                    // from its current cash balance

                } else if (plant.getActualInvestedCapital() * (1 - agent.getDebtRatioOfInvestments()) > agent
                        .getDownpaymentFractionOfCash() * agent.getCash()) {
                    logger.warn(
                            agent
                            + " will not invest in {} technology as he does not have enough money ({}) for downpayment",
                            technology, agent.getCash());
                    logger.warn("InvestedCapital," + plant.getActualInvestedCapital());

                    // If all above criteria passed: continue evaluation of
                    // investment option

                } else if (agent.getCash() < 0) {
                    logger.warn("Agent {} decides for no investment, because has {} cash", agent, agent.getCash());

                } else {



                    // Get relevant expected fuel prices and expected CO2 price
                    // And calculate the expected marginal cost in year
                    // futureTimepoint

                    Map<Substance, Double> myFuelPrices = new HashMap<Substance, Double>();
                    for (Substance fuel : technology.getFuels()) {
                        myFuelPrices.put(fuel, expectedFuelPrices.get(fuel));
                    }
                    Set<SubstanceShareInFuelMix> fuelMix = calculateFuelMix(plant, myFuelPrices,
                            expectedCO2Price.get(market));
                    plant.setFuelMix(fuelMix);

                    double expectedMarginalCost = determineExpectedMarginalCost(plant, expectedFuelPrices,
                            expectedCO2Price.get(market));

                    // Initialize running hours and expected gross profit
                    // Make number of segments

                    double runningHours = 0d;
                    double expectedGrossProfit = 0d;

                    long numberOfSegments = reps.segmentRepository.count();

                    // TODO somehow the prices of long-term contracts could also
                    // be used here to determine the expected profit. Maybe not
                    // though...

                    // For each segment on the load-duration curve,
                    // get expected electricity price and length in hours

                    // AvgElPrice WERKT NOG NIET

                    // double AvgExpElPrice = 0;
                    // double sumAllSegmentLoads = 0;
                    //
                    // for (SegmentLoad segmentLoad :
                    // market.getLoadDurationCurve()) {
                    // sumAllSegmentLoads += segmentLoad.getCurrentLoad();
                    // }
                    //
                    // for (SegmentLoad segmentLoad :
                    // market.getLoadDurationCurve()) {
                    // double expectedElectricityPrice =
                    // marketInformation.expectedElectricityPricesPerSegment
                    // .get(segmentLoad.getSegment());
                    // AvgExpElPrice += expectedElectricityPrice *
                    // segmentLoad.getCurrentLoad()
                    // / sumAllSegmentLoads;
                    //
                    // }

                    double ExpectedElectricityPrice_Average = 0;
                    for (SegmentLoad segmentLoad : market.getLoadDurationCurve()) {
                        double expectedElectricityPrice = marketInformation.expectedElectricityPricesPerSegment
                                .get(segmentLoad.getSegment());
                        double hours = segmentLoad.getSegment().getLengthInHours();
                        ExpectedElectricityPrice_Average += expectedElectricityPrice * (hours / 8760);
                    }

                    for (SegmentLoad segmentLoad : market.getLoadDurationCurve()) {
                        double expectedElectricityPrice = marketInformation.expectedElectricityPricesPerSegment
                                .get(segmentLoad.getSegment());

                        // logger.warn("segment," + segmentLoad.getSegment() +
                        // ",elPrice," + expectedElectricityPrice);
                        double hours = segmentLoad.getSegment().getLengthInHours();

                        // If expected marginal costs are lower than expected
                        // electricity price
                        // add the hours to the amount of expected running hours
                        // add the difference between price and MC times the
                        // amount of hours and
                        // available capacity (gross profit from serving
                        // expected demand in that segment)

                        if (expectedMarginalCost <= expectedElectricityPrice) {
                            runningHours += hours;

                            // available capacity for intermittent is nominal
                            // times
                            // Node load factor (based on weather
                            // circumstances?)

                            if (technology.isIntermittent())
                                expectedGrossProfit += (expectedElectricityPrice - expectedMarginalCost)
                                * hours
                                * plant.getActualNominalCapacity()
                                * reps.intermittentTechnologyNodeLoadFactorRepository
                                .findIntermittentTechnologyNodeLoadFactorForNodeAndTechnology(node,
                                        technology).getLoadFactorForSegment(segmentLoad.getSegment());

                            // If not intermittent available capacity is
                            // calculated by method
                            // getAvailable Capacity

                            else
                                expectedGrossProfit += (expectedElectricityPrice - expectedMarginalCost)
                                * hours
                                * plant.getAvailableCapacity(futureTimePoint, segmentLoad.getSegment(),
                                        numberOfSegments);
                        }

                    }

                    double averageExpectedElectricityPrice = expectedGrossProfit
                            / (runningHours * plant.getActualNominalCapacity()) + expectedMarginalCost;

                    // logger.warn("time," + getCurrentTick() + ",tech," +
                    // plant.getTechnology() + ",expElPrice,"
                    // + AvgExpElPrice
                    // + ",cap," + plant.getActualNominalCapacity() +
                    // ",expectedMC,"
                    // + expectedMarginalCost + ",runningHours," +
                    // runningHours);

                    // logger.warn(agent +
                    // "expects technology {} to have {} running", technology,
                    // runningHours);
                    // expect to meet minimum running hours?
                    if (runningHours < plant.getTechnology().getMinimumRunningHours()) {
                        // logger.warn(agent+
                        // " will not invest in {} technology as he expect to have {} running, which is lower then required",
                        // technology, runningHours);
                    } else {

                        // fixed operating costs calculated

                        double fixedOMCost = calculateFixedOperatingCost(plant, getCurrentTick());// /

                        // plant.getActualNominalCapacity();

                        double wacc = (1 - agent.getDebtRatioOfInvestments()) * agent.getEquityInterestRate()
                                + agent.getDebtRatioOfInvestments() * agent.getLoanInterestRate();

                        double operatingProfit = expectedGrossProfit - fixedOMCost;

                        TreeMap<Integer, Double> projectCapitalOutflow = calculateSimplePowerPlantInvestmentCashFlow(
                                technology.getDepreciationTime(), (int) plant.getActualLeadtime(),
                                plant.getActualInvestedCapital(), 0);

                        TreeMap<Integer, Double> projectCashInflow = calculateSimplePowerPlantInvestmentCashFlow(
                                technology.getDepreciationTime(), (int) plant.getActualLeadtime(), 0, operatingProfit);

                        double discountedCapitalCosts = npv(projectCapitalOutflow, wacc);

                        double discountedOpProfit = npv(projectCashInflow, wacc);

                        double projectValue = discountedOpProfit + discountedCapitalCosts;
                        double oldProjectValue = projectValue;

                        // ====== start Risk Aversion Part ====

                        Double[] arrayStandGrossOpProfit;

                        // logger.warn("================================================");
                        //
                        // logger.warn("tick," + getCurrentTick() + ",agent," +
                        // agent.getName() + ",tech,"
                        // + plant.getTechnology() + ",cap," +
                        // plant.getActualNominalCapacity());

                        // Get array of standardised gross profits by
                        // existing pp with same tech during past 5 years

                        arrayStandGrossOpProfit = reps.financialPowerPlantReportRepository
                                .getListOfStandardisedGrossOpProfitsOfPPownedByAgentWithTech(getCurrentTick() - 5,
                                        getCurrentTick(), agent, technology);

                        // Create array of gross operational profits by
                        // multiplying with nom capacity investment option

                        if (arrayStandGrossOpProfit == null) {
                            logger.warn("ArrayStandGrossProfit is Null");
                            Double[] arrayProjectValue = {
                                    oldProjectValue - oldProjectValue
                                    * agent.getHistoricalCVarPropensityForNewTechnologies(),
                                    oldProjectValue + oldProjectValue
                                    * agent.getHistoricalCVarPropensityForNewTechnologies() };

                            double riskPremium = 0;

                            if (agent.getRiskAversionType() == 0) {

                                projectValue = oldProjectValue;

                            }

                            if (agent.getRiskAversionType() == 1) {

                                double riskAversionCoefficient = agent.getRiskAversionCoefficientCARA();
                                riskPremium = calculateRiskPremiumCARA(riskAversionCoefficient, agent.getCash(),
                                        arrayProjectValue);

                                projectValue += riskPremium;

                                // logger.warn("time," + getCurrentTick() +
                                // ",tech," + plant.getTechnology()
                                // +"dCapCost,"+discountedCapitalCosts +
                                // ",RP," + riskPremium
                                // + ",percRPofCapCost," +
                                // riskPremium/discountedCapitalCosts);

                                // logger.warn("RP," + riskPremium + ",OPV,"
                                // + oldProjectValue + ",NPV,"
                                // + projectValue);

                            }

                            if (agent.getRiskAversionType() == 2) {

                                double riskAversionCoefficient = agent.getRiskAversionCoefficientCRRA();
                                riskPremium = calculateRiskPremiumCRRA(riskAversionCoefficient, agent.getCash(),
                                        arrayProjectValue);

                                if (riskPremium > 0) {
                                    riskPremium = 0;
                                }

                                projectValue += riskPremium;

                                // logger.warn("RP," + riskPremium + ",OPV,"
                                // + oldProjectValue + ",NPV,"
                                // + projectValue);

                            }

                            if (agent.getRiskAversionType() == 3) {

                                riskPremium = calculateRiskPremiumCVAR(agent.getHistoricalCVarAlpha(),
                                        arrayProjectValue);

                                projectValue += riskPremium;

                                // logger.warn("RP," + riskPremium + ",OPV,"
                                // + oldProjectValue + ",NPV,"
                                // + projectValue);

                            }

                        }

                        if (arrayStandGrossOpProfit != null) {

                            Double[] arrayGrossOpProfit = new Double[arrayStandGrossOpProfit.length];
                            for (int i = 0; i < arrayGrossOpProfit.length; i++) {
                                arrayGrossOpProfit[i] = arrayStandGrossOpProfit[i] * plant.getActualNominalCapacity();
                            }

                            // logger.warn("tick," + getCurrentTick() +
                            // ",agent," + agent.getName() + ",tech,"
                            // + plant.getTechnology() + ",wacc," + wacc +
                            // ",capacity,"
                            // + plant.getActualNominalCapacity() +
                            // ",depTime,"
                            // + technology.getDepreciationTime() +
                            // ",leadTime," + plant.getActualLeadtime()
                            // + ",discCapCost," + discountedCapitalCosts +
                            // ",fixedOMcosts" + fixedOMCost);

                            // logger.warn("EGP," + expectedGrossProfit +
                            // ",GParray,"
                            // + Arrays.toString(arrayGrossOpProfit));

                            // Create array of operational profits by
                            // deducting
                            // fixed OM costs

                            Double[] arrayOperationalProfit = new Double[arrayGrossOpProfit.length];
                            for (int i = 0; i < arrayOperationalProfit.length; i++) {
                                arrayOperationalProfit[i] = arrayGrossOpProfit[i] - fixedOMCost;
                            }

                            Double[] arrayDiscountedOpProfit = new Double[arrayOperationalProfit.length];
                            for (int i = 0; i < arrayDiscountedOpProfit.length; i++) {

                                TreeMap<Integer, Double> arrayProjectCashInflow = calculateSimplePowerPlantInvestmentCashFlow(
                                        technology.getDepreciationTime(), (int) plant.getActualLeadtime(), 0,
                                        arrayOperationalProfit[i]);

                                arrayDiscountedOpProfit[i] = npv(arrayProjectCashInflow, wacc);

                                arrayProjectCashInflow.clear();
                            }

                            // Create new array project value

                            Double[] arrayProjectValue = new Double[arrayDiscountedOpProfit.length];
                            for (int i = 0; i < arrayDiscountedOpProfit.length; i++) {
                                arrayProjectValue[i] = arrayDiscountedOpProfit[i] + discountedCapitalCosts;
                            }





                            // Create array for impact of arrayProjectValue
                            // on current cash balance

                            // if (testArrayStandGrossOpProfit != null) {
                            // logger.warn("Tick," + getCurrentTick() +
                            // ",Tech,"
                            // + plant.getTechnology()
                            // + ",TestArray, " +
                            // Arrays.toString(testArrayStandGrossOpProfit));
                            // }

                            double riskPremium = 0;

                            // risk aversionType: 0 risk neutral, 1 CARA, 2
                            // CRRA, 3 CVAR

                            if (agent.getRiskAversionType() == 0) {

                                projectValue = oldProjectValue;

                            }

                            if (agent.getRiskAversionType() == 1) {

                                double riskAversionCoefficient = agent.getRiskAversionCoefficientCARA();
                                riskPremium = calculateRiskPremiumCARA(riskAversionCoefficient, agent.getCash(),
                                        arrayProjectValue);

                                projectValue += riskPremium;

                                // logger.warn("time," + getCurrentTick() +
                                // ",tech," + plant.getTechnology()
                                // + ",dCapCost," + discountedCapitalCosts +
                                // ",RP," + riskPremium
                                // + ",percRPofCapCost," + riskPremium /
                                // discountedCapitalCosts);

                                // logger.warn("Cash," + agent.getCash() +
                                // ",RAcoeff," + riskAversionCoefficient
                                // + "riskPremium," + riskPremium + ",aPV,"
                                // + Arrays.toString(arrayProjectValue));

                                // logger.warn("RP," + riskPremium + ",OPV,"
                                // + oldProjectValue + ",NPV,"
                                // + projectValue);

                            }

                            if (agent.getRiskAversionType() == 2) {

                                double riskAversionCoefficient = agent.getRiskAversionCoefficientCRRA();
                                riskPremium = calculateRiskPremiumCRRA(riskAversionCoefficient, agent.getCash(),
                                        arrayProjectValue);
                                if (riskPremium > 0) {
                                    riskPremium = 0;
                                }
                                projectValue += riskPremium;



                                // logger.warn("time," + getCurrentTick() +
                                // ",tech," + plant.getTechnology()
                                // + ",dCapCost," + discountedCapitalCosts +
                                // ",RP," + riskPremium
                                // + ",percRPofCapCost," + riskPremium /
                                // discountedCapitalCosts);

                                // logger.warn("RP," + riskPremium + ",OPV,"
                                // + oldProjectValue + ",NPV,"
                                // + projectValue);

                            }

                            if (agent.getRiskAversionType() == 3) {

                                riskPremium = 0.07 * calculateRiskPremiumCVAR(agent.getHistoricalCVarAlpha(),
                                        arrayProjectValue);

                                projectValue += riskPremium;

                                // logger.warn("time," + getCurrentTick() +
                                // ",tech," + plant.getTechnology()
                                // + ",dCapCost," + discountedCapitalCosts +
                                // ",RP," + riskPremium
                                // + ",percRPofCapCost," + riskPremium /
                                // discountedCapitalCosts);

                                // logger.warn("tech," +
                                // plant.getTechnology() + ",arrayLength,"
                                // + arrayProjectValue.length);

                                // logger.warn("RP," + riskPremium + ",OPV,"
                                // + oldProjectValue + ",NPV,"
                                // + projectValue);

                            }

                            // Double[] arrayProjectValueImpactOnCash = new
                            // Double[arrayProjectValue.length];
                            // for (int i = 0; i <
                            // arrayProjectValueImpactOnCash.length; i++) {
                            // arrayProjectValueImpactOnCash[i] =
                            // arrayProjectValue[i] + agent.getCash();
                            // }
                            //
                            // double sumArrayProjectValueImpactOnCash = 0;
                            // for (int i = 0; i <
                            // arrayProjectValueImpactOnCash.length; i++) {
                            // sumArrayProjectValueImpactOnCash +=
                            // arrayProjectValueImpactOnCash[i];
                            // }
                            //
                            // double expectedImpactOnCash =
                            // sumArrayProjectValueImpactOnCash
                            // / arrayProjectValueImpactOnCash.length;

                            logger.warn("time," + getCurrentTick() + ",tech," + plant.getTechnology() + ",agent,"
                                    + agent.getName() + ",dOpProfit," + discountedOpProfit + ",dCapCost,"
                                    + discountedCapitalCosts + ",RP," + riskPremium + ",oldProjValue,"
                                    + oldProjectValue + ",newProjValue," + projectValue + ",nomCap,"
                                    + plant.getActualNominalCapacity() + ",runID," + model.getRunID()
                                    + ",runningHours," + runningHours + ",GOP," + expectedGrossProfit
                                    + ",expectedMarginalCosts," + expectedMarginalCost
                                    + ",expectedAvgElectricityPrice," + averageExpectedElectricityPrice
                                    + ",averageExpElPriceMarket," + ExpectedElectricityPrice_Average
                                    + ",InvestmentCosts," + plant.getActualInvestedCapital() + ",fixedOMcosts,"
                                    + fixedOMCost + ",investmentRound," + model.getInvestmentRound());

                            // logger.warn("RiskPremium:" + riskPremium +
                            // "fixedOMcosts," + fixedOMCost
                            // + ",expGrossProfit," + expectedGrossProfit +
                            // ",oldPV," + oldProjectValue);
                            // logger.warn("aSGOP," +
                            // Arrays.toString(arrayStandGrossOpProfit));
                            // logger.warn("aGOP," +
                            // Arrays.toString(arrayGrossOpProfit));
                            // logger.warn("aOP," +
                            // Arrays.toString(arrayOperationalProfit));
                            // // logger.warn("aDOP," +
                            // // Arrays.toString(arrayDiscountedOpProfit));
                            // logger.warn("aPV," +
                            // Arrays.toString(arrayProjectValue));

                        }


                        if (projectValue > 0 && projectValue / plant.getActualNominalCapacity() > highestValue) {
                            highestValue = projectValue / plant.getActualNominalCapacity();
                            bestTechnology = plant.getTechnology();
                            bestNode = node;
                        }

                        if (oldProjectValue > 0
                                && oldProjectValue / plant.getActualNominalCapacity() > highestValueWithoutRiskPremium) {
                            highestValueWithoutRiskPremium = oldProjectValue / plant.getActualNominalCapacity();
                            bestTechnologyWithoutRiskPremium = plant.getTechnology();
                            bestNodeWithoutRiskPremium = node;
                        }
                    }

                }

            }
        }

        if (bestTechnology != null && bestTechnologyWithoutRiskPremium != null
                && !bestTechnologyWithoutRiskPremium.equals(bestTechnology)) {
            logger.warn("time," + getCurrentTick() + ",HaveInv," + bestTechnology + ",WouldHaveInv,"
                    + bestTechnologyWithoutRiskPremium.getName());
        }
        if (bestTechnology == null && bestTechnologyWithoutRiskPremium != null) {
            logger.warn("time," + getCurrentTick() + ",WouldHave," + bestTechnologyWithoutRiskPremium.getName()
                    + ",NotInv," + 1);
        }

        if (bestTechnology != null) {

            PowerPlant plant = new PowerPlant();
            plant.specifyAndPersist(getCurrentTick(), agent, bestNode, bestTechnology);
            PowerPlantManufacturer manufacturer = reps.genericRepository.findFirst(PowerPlantManufacturer.class);
            BigBank bigbank = reps.genericRepository.findFirst(BigBank.class);

            logger.warn("time," + getCurrentTick() + ",NewInvest," + plant.getTechnology() + ",Cap,"
                    + plant.getActualNominalCapacity() + ",agent," + agent.getName());

            double investmentCostPayedByEquity = plant.getActualInvestedCapital()
                    * (1 - agent.getDebtRatioOfInvestments());
            double investmentCostPayedByDebt = plant.getActualInvestedCapital() * agent.getDebtRatioOfInvestments();
            double downPayment = investmentCostPayedByEquity;
            createSpreadOutDownPayments(agent, manufacturer, downPayment, plant);

            double amount = determineLoanAnnuities(investmentCostPayedByDebt, plant.getTechnology()
                    .getDepreciationTime(), agent.getLoanInterestRate());
            // logger.warn("Loan amount is: " + amount);
            Loan loan = reps.loanRepository.createLoan(agent, bigbank, amount, plant.getTechnology()
                    .getDepreciationTime(), getCurrentTick(), plant);
            // Create the loan
            plant.createOrUpdateLoan(loan);

        } else {
            // logger.warn("{} found no suitable technology anymore to invest in at tick "
            // + getCurrentTick(), agent);
            // agent will not participate in the next round of investment if
            // he does not invest now
            setNotWillingToInvest(agent);
        }
    }

    // ===================== End of investment loop =========================

    // }

    // Creates n downpayments of equal size in each of the n building years of a
    // power plant
    @Transactional
    private void createSpreadOutDownPayments(EnergyProducer agent, PowerPlantManufacturer manufacturer,
            double totalDownPayment, PowerPlant plant) {
        int buildingTime = (int) plant.getActualLeadtime();
        reps.nonTransactionalCreateRepository.createCashFlow(agent, manufacturer, totalDownPayment / buildingTime,
                CashFlow.DOWNPAYMENT, getCurrentTick(), plant);
        Loan downpayment = reps.loanRepository.createLoan(agent, manufacturer, totalDownPayment / buildingTime,
                buildingTime - 1, getCurrentTick(), plant);
        plant.createOrUpdateDownPayment(downpayment);
    }

    @Transactional
    private void setNotWillingToInvest(EnergyProducer agent) {
        agent.setWillingToInvest(false);
    }

    /**
     * Predicts fuel prices for {@link futureTimePoint} using a geometric trend
     * regression forecast. Only predicts fuels that are traded on a commodity
     * market.
     *
     * @param agent
     * @param futureTimePoint
     * @return Map<Substance, Double> of predicted prices.
     */
    public Map<Substance, Double> predictFuelPrices(EnergyProducer agent, long futureTimePoint) {
        // Fuel Prices
        Map<Substance, Double> expectedFuelPrices = new HashMap<Substance, Double>();
        for (Substance substance : reps.substanceRepository.findAllSubstancesTradedOnCommodityMarkets()) {
            // Find Clearing Points for the last 5 years (counting current year
            // as one of the last 5 years).
            Iterable<ClearingPoint> cps = reps.clearingPointRepository
                    .findAllClearingPointsForSubstanceTradedOnCommodityMarkesAndTimeRange(substance, getCurrentTick()
                            - (agent.getNumberOfYearsBacklookingForForecasting() - 1), getCurrentTick(), false);
            // logger.warn("{}, {}",
            // getCurrentTick()-(agent.getNumberOfYearsBacklookingForForecasting()-1),
            // getCurrentTick());
            // Create regression object
            SimpleRegression gtr = new SimpleRegression();
            for (ClearingPoint clearingPoint : cps) {
                // logger.warn("CP {}: {} , in" + clearingPoint.getTime(),
                // substance.getName(), clearingPoint.getPrice());
                gtr.addData(clearingPoint.getTime(), clearingPoint.getPrice());
            }
            gtr.addData(getCurrentTick(), findLastKnownPriceForSubstance(substance, getCurrentTick()));
            expectedFuelPrices.put(substance, gtr.predict(futureTimePoint));
            // logger.warn("Forecast {}: {}, in Step " + futureTimePoint,
            // substance, expectedFuelPrices.get(substance));
        }
        return expectedFuelPrices;
    }

    // Create a powerplant investment and operation cash-flow in the form of a
    // map. If only investment, or operation costs should be considered set
    // totalInvestment or operatingProfit to 0
    private TreeMap<Integer, Double> calculateSimplePowerPlantInvestmentCashFlow(int depriacationTime,
            int buildingTime, double totalInvestment, double operatingProfit) {
        TreeMap<Integer, Double> investmentCashFlow = new TreeMap<Integer, Double>();
        double equalTotalDownPaymentInstallement = totalInvestment / buildingTime;
        for (int i = 0; i < buildingTime; i++) {
            investmentCashFlow.put(new Integer(i), -equalTotalDownPaymentInstallement);
        }
        for (int i = buildingTime; i < depriacationTime + buildingTime; i++) {
            investmentCashFlow.put(new Integer(i), operatingProfit);
        }

        return investmentCashFlow;
    }

    private double npv(TreeMap<Integer, Double> netCashFlow, double wacc) {
        double npv = 0;
        for (Integer iterator : netCashFlow.keySet()) {
            npv += netCashFlow.get(iterator).doubleValue() / Math.pow(1 + wacc, iterator.intValue());
        }
        return npv;
    }

    private double calculateRiskPremiumCARA(double riskAversionCoefficient, double cashBalance,
            Double[] arrayEmpiricalProjectValues) {

        // initialize variables used in CARA method
        double riskPremium = 0;
        double caraCertaintyEquivalent = 0;
        double correctionFactor = cashBalance;
        double sumCARAutilities = 0;
        double meanCARAutilities = 0;
        double sumCARAprojValues = 0;
        double meanCARAexpectedProjValue = 0;

        Double[] cashBalancePlusEmpiricalProjValues = new Double[arrayEmpiricalProjectValues.length];

        for (int i = 0; i < cashBalancePlusEmpiricalProjValues.length; i++) {
            cashBalancePlusEmpiricalProjValues[i] = arrayEmpiricalProjectValues[i] + cashBalance;
        }

        for (int i = 0; i < cashBalancePlusEmpiricalProjValues.length; i++) {
            sumCARAprojValues = sumCARAprojValues + cashBalancePlusEmpiricalProjValues[i];
        }
        meanCARAexpectedProjValue = sumCARAprojValues / cashBalancePlusEmpiricalProjValues.length;

        Double[] arrayCARAutilitiesEmpProjValues = new Double[cashBalancePlusEmpiricalProjValues.length];

        for (int i = 0; i < arrayCARAutilitiesEmpProjValues.length; i++) {
            arrayCARAutilitiesEmpProjValues[i] = (1 / -riskAversionCoefficient)
                    * Math.exp(-riskAversionCoefficient * (cashBalancePlusEmpiricalProjValues[i] - correctionFactor))
                    + (1 / riskAversionCoefficient);
        }

        for (int i = 0; i < arrayCARAutilitiesEmpProjValues.length; i++) {
            sumCARAutilities = sumCARAutilities + arrayCARAutilitiesEmpProjValues[i];
        }

        meanCARAutilities = sumCARAutilities / arrayCARAutilitiesEmpProjValues.length;

        caraCertaintyEquivalent = Math.log(-riskAversionCoefficient * meanCARAutilities + 1)
                / (-riskAversionCoefficient) + correctionFactor;

        riskPremium += (caraCertaintyEquivalent - meanCARAexpectedProjValue);

        double count = 0;

        while (riskPremium == Double.POSITIVE_INFINITY) {

            logger.warn("Risk Premium is infinite!!");

            riskPremium = 0;
            sumCARAutilities = 0;
            riskAversionCoefficient = riskAversionCoefficient / 10;

            for (int i = 0; i < arrayCARAutilitiesEmpProjValues.length; i++) {
                arrayCARAutilitiesEmpProjValues[i] = (1 / -riskAversionCoefficient)
                        * Math.exp(-riskAversionCoefficient
                                * (cashBalancePlusEmpiricalProjValues[i] - correctionFactor))
                                + (1 / riskAversionCoefficient);
            }

            for (int i = 0; i < arrayCARAutilitiesEmpProjValues.length; i++) {
                sumCARAutilities = sumCARAutilities + arrayCARAutilitiesEmpProjValues[i];
            }

            meanCARAutilities = sumCARAutilities / arrayCARAutilitiesEmpProjValues.length;

            caraCertaintyEquivalent = Math.log(-riskAversionCoefficient * meanCARAutilities + 1)
                    / (-riskAversionCoefficient) + correctionFactor;

            riskPremium += (caraCertaintyEquivalent - meanCARAexpectedProjValue);

            count++;

            // logger.warn("Count: " + count + ", New Risk Aversion Factor: " +
            // newRiskAversionFactor);

        }

        /*
         * logger.warn("risk premium: " + riskPremium + "Cash: " + cashBalance +
         * ", worstCase: " + lowerROI + ", bestcase: " + higherROI +
         * ", expected: " + expectedROI);
         */

        return riskPremium;

    }

    private double calculateRiskPremiumCRRA(double riskAversionCoefficient, double cashBalance,
            Double[] arrayEmpiricalProjectValues) {

        double riskPremium = 0;
        double crraCertaintyEquivalent = 0;
        double sumCRRAutilities = 0;
        double meanCRRAutilities = 0;
        double sumCRRAprojValues = 0;
        double meanCRRAexpectedProjValue = 0;
        Double[] arrayCRRAutilitiesEmpProjValues = new Double[arrayEmpiricalProjectValues.length];

        Double[] cashBalancePlusEmpiricalProjValues = new Double[arrayEmpiricalProjectValues.length];

        for (int i = 0; i < cashBalancePlusEmpiricalProjValues.length; i++) {
            cashBalancePlusEmpiricalProjValues[i] = arrayEmpiricalProjectValues[i] + cashBalance;
            if (cashBalancePlusEmpiricalProjValues[i] < 0) {
                cashBalancePlusEmpiricalProjValues[i] = 1.1;
            }
        }

        for (int i = 0; i < cashBalancePlusEmpiricalProjValues.length; i++) {
            sumCRRAprojValues = sumCRRAprojValues + cashBalancePlusEmpiricalProjValues[i];
        }
        meanCRRAexpectedProjValue = sumCRRAprojValues / cashBalancePlusEmpiricalProjValues.length;

        if (meanCRRAexpectedProjValue < 0) {
            logger.warn("expectedPayoffRiskDistribution below zero," + meanCRRAexpectedProjValue);
        }

        if (riskAversionCoefficient == 1) {

            for (int i = 0; i < arrayCRRAutilitiesEmpProjValues.length; i++) {
                arrayCRRAutilitiesEmpProjValues[i] = Math.log(cashBalancePlusEmpiricalProjValues[i]);
            }
        } else {

            for (int i = 0; i < arrayCRRAutilitiesEmpProjValues.length; i++) {
                arrayCRRAutilitiesEmpProjValues[i] = Math.pow(cashBalancePlusEmpiricalProjValues[i],
                        1 - riskAversionCoefficient) / (1 - riskAversionCoefficient);
            }

        }

        for (int i = 0; i < arrayCRRAutilitiesEmpProjValues.length; i++) {
            sumCRRAutilities = sumCRRAutilities + arrayCRRAutilitiesEmpProjValues[i];
        }
        meanCRRAutilities = sumCRRAutilities / arrayCRRAutilitiesEmpProjValues.length;

        if (riskAversionCoefficient == 1) {
            crraCertaintyEquivalent = Math.exp(meanCRRAutilities);
        } else {
            crraCertaintyEquivalent = Math.pow(meanCRRAutilities * (1 - riskAversionCoefficient),
                    1 / (1 - riskAversionCoefficient));

        }

        riskPremium += (crraCertaintyEquivalent - meanCRRAexpectedProjValue);

        // logger.warn("risk premium: " + riskPremium + "Cash: " + cashBalance +
        // ", worstCase: " + lowerROI
        // + ", bestcase: " + higherROI + ", expected: " + expectedROI);

        return riskPremium;

    }

    private double calculateRiskPremiumCVAR(double alpha, Double[] arrayEmpiricalProjectValues) {
        double riskPremium = 0;
        double sumLowestAlphaPercent = 0;
        double meanLowestAlphaPercent = 0;

        int lowestFivePercent = (int) (alpha * arrayEmpiricalProjectValues.length);

        if (lowestFivePercent == 0) {
            lowestFivePercent = 1;
        }

        for (int i = 0; i < lowestFivePercent; i++) {
            sumLowestAlphaPercent = sumLowestAlphaPercent + arrayEmpiricalProjectValues[i];
        }
        meanLowestAlphaPercent = sumLowestAlphaPercent / lowestFivePercent;

        if (meanLowestAlphaPercent < 0) {
            riskPremium = meanLowestAlphaPercent;
        }

        // logger.warn("lfp," + lowestFivePercent + ",mlfp," +
        // meanLowestAlphaPercent);
        return riskPremium;
    }

    public double determineExpectedMarginalCost(PowerPlant plant, Map<Substance, Double> expectedFuelPrices,
            double expectedCO2Price) {
        double mc = determineExpectedMarginalFuelCost(plant, expectedFuelPrices);
        double co2Intensity = plant.calculateEmissionIntensity();
        mc += co2Intensity * expectedCO2Price;
        return mc;
    }

    public double determineExpectedMarginalFuelCost(PowerPlant powerPlant, Map<Substance, Double> expectedFuelPrices) {
        double fc = 0d;
        for (SubstanceShareInFuelMix mix : powerPlant.getFuelMix()) {
            double amount = mix.getShare();
            double fuelPrice = expectedFuelPrices.get(mix.getSubstance());
            fc += amount * fuelPrice;
        }
        return fc;
    }

    private PowerGridNode getNodeForZone(Zone zone) {
        for (PowerGridNode node : reps.genericRepository.findAll(PowerGridNode.class)) {
            if (node.getZone().equals(zone)) {
                return node;
            }
        }
        return null;
    }

    private class MarketInformation {

        Map<Segment, Double> expectedElectricityPricesPerSegment;
        double maxExpectedLoad = 0d;
        Map<PowerPlant, Double> meritOrder;
        double capacitySum;

        MarketInformation(ElectricitySpotMarket market, Map<ElectricitySpotMarket, Double> expectedDemand,
                Map<Substance, Double> fuelPrices, double co2price, long time) {
            // determine expected power prices
            expectedElectricityPricesPerSegment = new HashMap<Segment, Double>();
            Map<PowerPlant, Double> marginalCostMap = new HashMap<PowerPlant, Double>();
            capacitySum = 0d;

            // get merit order for this market
            for (PowerPlant plant : reps.powerPlantRepository.findExpectedOperationalPowerPlantsInMarket(market, time)) {

                double plantMarginalCost = determineExpectedMarginalCost(plant, fuelPrices, co2price);
                marginalCostMap.put(plant, plantMarginalCost);
                capacitySum += plant.getActualNominalCapacity();
            }

            // get difference between technology target and expected operational
            // capacity
            for (TargetInvestor targetInvestor : reps.targetInvestorRepository.findAllByMarket(market)) {
                if (!(targetInvestor instanceof StochasticTargetInvestor)) {
                    for (PowerGeneratingTechnologyTarget pggt : targetInvestor.getPowerGenerationTechnologyTargets()) {
                        double expectedTechnologyCapacity = reps.powerPlantRepository
                                .calculateCapacityOfExpectedOperationalPowerPlantsInMarketAndTechnology(market,
                                        pggt.getPowerGeneratingTechnology(), time);
                        double targetDifference = pggt.getTrend().getValue(time) - expectedTechnologyCapacity;
                        if (targetDifference > 0) {
                            PowerPlant plant = new PowerPlant();
                            plant.specifyNotPersist(getCurrentTick(), new EnergyProducer(),
                                    reps.powerGridNodeRepository.findFirstPowerGridNodeByElectricitySpotMarket(market),
                                    pggt.getPowerGeneratingTechnology());
                            plant.setActualNominalCapacity(targetDifference);
                            double plantMarginalCost = determineExpectedMarginalCost(plant, fuelPrices, co2price);
                            marginalCostMap.put(plant, plantMarginalCost);
                            capacitySum += targetDifference;
                        }
                    }
                } else {
                    for (PowerGeneratingTechnologyTarget pggt : targetInvestor.getPowerGenerationTechnologyTargets()) {
                        double expectedTechnologyCapacity = reps.powerPlantRepository
                                .calculateCapacityOfExpectedOperationalPowerPlantsInMarketAndTechnology(market,
                                        pggt.getPowerGeneratingTechnology(), time);
                        double expectedTechnologyAddition = 0;
                        long contructionTime = getCurrentTick()
                                + pggt.getPowerGeneratingTechnology().getExpectedLeadtime()
                                + pggt.getPowerGeneratingTechnology().getExpectedPermittime();
                        for (long investmentTimeStep = contructionTime + 1; investmentTimeStep <= time; investmentTimeStep = investmentTimeStep + 1) {
                            expectedTechnologyAddition += (pggt.getTrend().getValue(investmentTimeStep) - pggt
                                    .getTrend().getValue(investmentTimeStep - 1));
                        }
                        if (expectedTechnologyAddition > 0) {
                            PowerPlant plant = new PowerPlant();
                            plant.specifyNotPersist(getCurrentTick(), new EnergyProducer(),
                                    reps.powerGridNodeRepository.findFirstPowerGridNodeByElectricitySpotMarket(market),
                                    pggt.getPowerGeneratingTechnology());
                            plant.setActualNominalCapacity(expectedTechnologyAddition);
                            double plantMarginalCost = determineExpectedMarginalCost(plant, fuelPrices, co2price);
                            marginalCostMap.put(plant, plantMarginalCost);
                            capacitySum += expectedTechnologyAddition;
                        }
                    }
                }

            }

            MapValueComparator comp = new MapValueComparator(marginalCostMap);
            meritOrder = new TreeMap<PowerPlant, Double>(comp);
            meritOrder.putAll(marginalCostMap);

            long numberOfSegments = reps.segmentRepository.count();

            double demandFactor = expectedDemand.get(market).doubleValue();

            // find expected prices per segment given merit order
            for (SegmentLoad segmentLoad : market.getLoadDurationCurve()) {

                double expectedSegmentLoad = segmentLoad.getBaseLoad() * demandFactor;

                if (expectedSegmentLoad > maxExpectedLoad) {
                    maxExpectedLoad = expectedSegmentLoad;
                }

                double segmentSupply = 0d;
                double segmentPrice = 0d;
                double totalCapacityAvailable = 0d;

                for (Entry<PowerPlant, Double> plantCost : meritOrder.entrySet()) {
                    PowerPlant plant = plantCost.getKey();
                    double plantCapacity = 0d;
                    // Determine available capacity in the future in this
                    // segment
                    plantCapacity = plant
                            .getExpectedAvailableCapacity(time, segmentLoad.getSegment(), numberOfSegments);
                    totalCapacityAvailable += plantCapacity;
                    // logger.warn("Capacity of plant " + plant.toString() +
                    // " is " +
                    // plantCapacity/plant.getActualNominalCapacity());
                    if (segmentSupply < expectedSegmentLoad) {
                        segmentSupply += plantCapacity;
                        segmentPrice = plantCost.getValue();
                    }

                }

                // logger.warn("Segment " +
                // segmentLoad.getSegment().getSegmentID() + " supply equals " +
                // segmentSupply + " and segment demand equals " +
                // expectedSegmentLoad);

                // Find strategic reserve operator for the market.
                double reservePrice = 0;
                double reserveVolume = 0;
                for (StrategicReserveOperator operator : strategicReserveOperatorRepository.findAll()) {
                    ElectricitySpotMarket market1 = reps.marketRepository.findElectricitySpotMarketForZone(operator
                            .getZone());
                    if (market.getNodeId().intValue() == market1.getNodeId().intValue()) {
                        reservePrice = operator.getReservePriceSR();
                        reserveVolume = operator.getReserveVolume();
                    }
                }

                if (segmentSupply >= expectedSegmentLoad
                        && ((totalCapacityAvailable - expectedSegmentLoad) <= (reserveVolume))) {
                    expectedElectricityPricesPerSegment.put(segmentLoad.getSegment(), reservePrice);
                    // logger.warn("Price: "+
                    // expectedElectricityPricesPerSegment);
                } else if (segmentSupply >= expectedSegmentLoad
                        && ((totalCapacityAvailable - expectedSegmentLoad) > (reserveVolume))) {
                    expectedElectricityPricesPerSegment.put(segmentLoad.getSegment(), segmentPrice);
                    // logger.warn("Price: "+
                    // expectedElectricityPricesPerSegment);
                } else {
                    expectedElectricityPricesPerSegment.put(segmentLoad.getSegment(), market.getValueOfLostLoad());
                }

            }
        }
    }

    /**
     * Calculates expected CO2 price based on a geometric trend estimation, of
     * the past years. The adjustmentForDetermineFuelMix needs to be set to 1,
     * if this is used in the determine fuel mix role.
     *
     * @param futureTimePoint
     *            Year the prediction is made for
     * @param yearsLookingBackForRegression
     *            How many years are used as input for the regression, incl. the
     *            current tick.
     * @return
     */
    protected HashMap<ElectricitySpotMarket, Double> determineExpectedCO2PriceInclTaxAndFundamentalForecast(
            long futureTimePoint, long yearsLookingBackForRegression, int adjustmentForDetermineFuelMix,
            long clearingTick) {
        HashMap<ElectricitySpotMarket, Double> co2Prices = new HashMap<ElectricitySpotMarket, Double>();
        CO2Auction co2Auction = reps.genericRepository.findFirst(CO2Auction.class);
        Iterable<ClearingPoint> cps = reps.clearingPointRepository.findAllClearingPointsForMarketAndTimeRange(
                co2Auction, clearingTick - yearsLookingBackForRegression + 1 - adjustmentForDetermineFuelMix,
                clearingTick - adjustmentForDetermineFuelMix, false);
        // Create regression object and calculate average
        SimpleRegression sr = new SimpleRegression();
        Government government = reps.template.findAll(Government.class).iterator().next();
        double lastPrice = 0;
        double averagePrice = 0;
        int i = 0;
        for (ClearingPoint clearingPoint : cps) {
            sr.addData(clearingPoint.getTime(), clearingPoint.getPrice());
            lastPrice = clearingPoint.getPrice();
            averagePrice += lastPrice;
            i++;
        }
        averagePrice = averagePrice / i;
        double expectedCO2Price;
        double expectedRegressionCO2Price;
        if (i > 1) {
            expectedRegressionCO2Price = sr.predict(futureTimePoint);
            expectedRegressionCO2Price = Math.max(0, expectedRegressionCO2Price);
            expectedRegressionCO2Price = Math
                    .min(expectedRegressionCO2Price, government.getCo2Penalty(futureTimePoint));
        } else {
            expectedRegressionCO2Price = lastPrice;
        }
        ClearingPoint expectedCO2ClearingPoint = reps.clearingPointRepository.findClearingPointForMarketAndTime(
                co2Auction, getCurrentTick()
                + reps.genericRepository.findFirst(DecarbonizationModel.class).getCentralForecastingYear(),
                true);
        expectedCO2Price = (expectedCO2ClearingPoint == null) ? 0 : expectedCO2ClearingPoint.getPrice();
        expectedCO2Price = (expectedCO2Price + expectedRegressionCO2Price) / 2;
        for (ElectricitySpotMarket esm : reps.marketRepository.findAllElectricitySpotMarkets()) {
            double nationalCo2MinPriceinFutureTick = reps.nationalGovernmentRepository
                    .findNationalGovernmentByElectricitySpotMarket(esm).getMinNationalCo2PriceTrend()
                    .getValue(futureTimePoint);
            double co2PriceInCountry = 0d;
            if (expectedCO2Price > nationalCo2MinPriceinFutureTick) {
                co2PriceInCountry = expectedCO2Price;
            } else {
                co2PriceInCountry = nationalCo2MinPriceinFutureTick;
            }
            co2PriceInCountry += reps.genericRepository.findFirst(Government.class).getCO2Tax(futureTimePoint);
            co2Prices.put(esm, Double.valueOf(co2PriceInCountry));
        }
        return co2Prices;
    }

}