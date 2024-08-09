            //       let all_nodes = {}
            //       for (const [key, value] of Object.entries(network.body.nodes)){
            //         all_nodes[key] = value.options.table_name;
            //     }

            //   function makeTableBigger(node) {
            //     // const result = Object.keys(all_nodes).filter(key, value)
            //     const matchingKeys = Object.keys(all_nodes).filter(key => all_nodes[key] === 'wfm.dimemployee_se');

            //     let colors = ['red', 'green', 'blue'];
            //     for (const id of matchingKeys){
            //         nodes.update({id:id , size: 500});
            //         ;
            //     }
            //   }



              let all_nodes = {}
              for (const [key, value] of Object.entries(network.body.nodes)){
                all_nodes[key] = value.options.table_name;
            }

          function makeTableBigger(node) {
            // const result = Object.keys(all_nodes).filter(key, value)
            const matchingKeys = Object.keys(all_nodes).filter(key => all_nodes[key] === 'wfm.dimemployee_se');

            let colors = ['red', 'green', 'blue'];
            for (const id of matchingKeys){
                nodes.update({id:id , size: 500});
                ;
            }
          }



          function getNodeList() {
            let distinct_tables = []
            for (const [key, value] of Object.entries(network.body.nodes)){
                if (!distinct_tables.includes(value.options.table_name)) {
              distinct_tables.push(value.options.table_name);
            }
          }
          return distinct_tables
        }


        function populateSelectOptions() {
            const selectNode = document.getElementById('select-node');
            const nodes = getNodeList();
    
            nodes.forEach(node => {
                const option = document.createElement('option');
                option.value = node;
                option.textContent = node;
                selectNode.appendChild(option);
            });
        }
    