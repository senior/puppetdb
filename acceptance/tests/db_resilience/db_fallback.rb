if databases.count > 1
  test_name "test database fallback" do

    step "clear puppetdb databases" do
      databases.each do |database|
        clear_and_restart_puppetdb(database)
      end
    end

    with_puppet_running_on master, {
      'master' => {
        'autosign' => 'true'
      }} do
        step "Run agents once to activate nodes" do
          run_agent_on agents, "--test --server #{master}"
        end
      end

      step "Verify that the number of active nodes is what we expect" do
        result = on databases[0], %Q|curl -G http://localhost:8080/v3/nodes|
        parsed_result = JSON.parse(result.stdout)
        assert_equal(agents.length, parsed_result.length,
                     "Expected query to return '#{agents.length}' active nodes; returned '#{parsed_result.length}'")
      end

      step "shut down database" do
        stop_puppetdb(databases[0])
      end

      step "check that the fallback db is responsive to queries and has no nodes" do
        result = on databases[1], %Q|curl -G http://localhost:8080/v3/nodes|
        parsed_result = JSON.parse(result.stdout)
        assert_equal(0, parsed_result.length,
                     "Expected query to return 0 active nodes; returned '#{parsed_result.length}'")
      end

      with_puppet_running_on master, {
        'master' => {
          'autosign' => 'true'
        }} do
          step "attempt to populate database with first host down" do
            run_agent_on agents, "--test --server #{master}"
          end
        end

        step "Verify that fallback occurred" do
          result = on databases[1], %Q|curl -G http://localhost:8080/v3/nodes|
          parsed_result = JSON.parse(result.stdout)
          assert_equal(agents.length, parsed_result.length,
                       "Expected query to return '#{agents.length}' active nodes; returned '#{parsed_result.length}'")
        end


        step "clearn and restart primary database" do
          clear_and_restart_puppetdb(databases[0])
        end

        with_puppet_running_on master, {
          'master' => {
            'autosign' => 'true'
          }} do
            step "run agents to populate db again" do
              run_agent_on agents, "--test --server #{master}"
            end
          end

          step "check that the first db is responsive to queries" do
            result = on databases[0], %Q|curl -G http://localhost:8080/v3/nodes|
            parsed_result = JSON.parse(result.stdout)
            assert_equal(agents.length, parsed_result.length,
                         "Expected query to return '#{agents.length}' active nodes; returned '#{parsed_result.length}'")
          end
  end
end
