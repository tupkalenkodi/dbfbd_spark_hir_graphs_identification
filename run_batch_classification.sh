#!/bin/bash
# run_batch_classification.sh

for n in 10 11 12 13; do
    echo ""
    echo "###################################################################"
    echo "# Starting classification for n=$n"
    echo "###################################################################"
    echo ""

    ./run_classification.sh $n

    if [ $? -eq 0 ]; then
        echo " n=$n completed successfully"
    else
        echo " n=$n failed"
        exit 1
    fi

    echo ""
done

echo ""
echo "###################################################################"
echo "# All classifications completed!"
echo "###################################################################"