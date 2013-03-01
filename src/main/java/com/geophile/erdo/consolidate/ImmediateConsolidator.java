package com.geophile.erdo.consolidate;

class ImmediateConsolidator extends Consolidator
{
    // ImmediateConsolidator interface

    public static ImmediateConsolidator newConsolidator(ConsolidationSet consolidationSet,
                                                        ConsolidationPlanner planner)
    {
        return new ImmediateConsolidator(consolidationSet, planner);
    }

    // For use by this class

    private ImmediateConsolidator(ConsolidationSet consolidationSet, ConsolidationPlanner planner)
    {
        super(consolidationSet, planner);
    }

}
