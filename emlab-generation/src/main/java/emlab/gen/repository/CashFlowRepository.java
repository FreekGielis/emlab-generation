/*******************************************************************************
 * Copyright 2012 the original author or authors.
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
package emlab.gen.repository;

import org.springframework.data.neo4j.annotation.Query;
import org.springframework.data.neo4j.annotation.QueryType;
import org.springframework.data.neo4j.repository.GraphRepository;
import org.springframework.data.repository.query.Param;

import emlab.gen.domain.contract.CashFlow;
import emlab.gen.domain.technology.PowerPlant;

public interface CashFlowRepository extends GraphRepository<CashFlow> {
    @Query("START cf=node:__types__(\"className:emlab.gen.domain.contract.CashFlow\") WHERE (cf.time={time}) RETURN cf")
    Iterable<CashFlow> findAllCashFlowsForForTime(@Param("time") long time);

    @Query(value = "g.v(plant).in.filter{it.__type__=='emlab.gen.domain.contract.CashFlow' && it.time==tick}", type = QueryType.Gremlin)
    Iterable<CashFlow> findAllCashFlowsForPowerPlantForTime(@Param("plant") PowerPlant plant, @Param("tick") long tick);

}
