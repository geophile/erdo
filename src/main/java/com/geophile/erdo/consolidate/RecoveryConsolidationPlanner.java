package com.geophile.erdo.consolidate;

import java.util.List;

import static com.geophile.erdo.consolidate.Consolidation.Element;

public class RecoveryConsolidationPlanner extends ConsolidationPlanner
{
    // ConsolidationPlanner interface

    @Override
    public boolean planConsolidation(Element newElement)
    {
        return true;
    }

    @Override
    public List<Element> elementsToConsolidate()
    {
        return consolidationElements;
    }

    @Override
    public String type()
    {
        return "recovery";
    }

    // AllNonDurableConsolidationPlanner interface

    public static RecoveryConsolidationPlanner newPlanner(ConsolidationSet consolidationSet,
                                                          List<Element> consolidationElements)
    {
        return new RecoveryConsolidationPlanner(consolidationSet, consolidationElements);
    }

    // For use by this class

    private RecoveryConsolidationPlanner(ConsolidationSet consolidationSet, List<Element> consolidationElements)
    {
        super(consolidationSet, true, true);
        this.consolidationElements = consolidationElements;
    }

    // Object state

    private List<Element> consolidationElements;
}
