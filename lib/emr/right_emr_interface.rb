#
# Copyright (c) 2011 RightScale Inc
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
# LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
# WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#

module RightAws

  # = RightAWS::EmrInterface -- RightScale Amazon Elastic Map Reduce interface
  # The RightAws::AsInterface class provides a complete interface to Amazon
  # Elastic Map Reduce service.
  #
  # For explanations of the semantics of each call, please refer to Amazon's
  # documentation at
  # http://aws.amazon.com/documentation/elasticmapreduce/
  #
  # Create an interface handle:
  #
  #  emr = RightAws::EmrInterface.new(aws_access_key_id, aws_secret_access_key)
  #
  # Create a launch configuration:
  #
  #  as.create_launch_configuration('CentOS.5.1-c', 'ami-08f41161', 'm1.small',
  #                                 :key_name        => 'kd-moo-test',
  #                                 :security_groups => ['default'],
  #                                 :user_data       => "Woohoo: CentOS.5.1-c" )
  #
  # Create an AutoScaling group:
  #
  #  as.create_auto_scaling_group('CentOS.5.1-c-array', 'CentOS.5.1-c', 'us-east-1c',
  #                               :min_size => 2,
  #                               :max_size => 5)
  #
  # Create a new trigger:
  # 
  #  as.create_or_update_scaling_trigger('kd.tr.1', 'CentOS.5.1-c-array',
  #                                      :measure_name => 'CPUUtilization',
  #                                      :statistic => :average,
  #                                      :dimensions => {
  #                                         'AutoScalingGroupName' => 'CentOS.5.1-c-array',
  #                                         'Namespace' => 'AWS',
  #                                         'Service' => 'EC2' },
  #                                      :period => 60,
  #                                      :lower_threshold => 5,
  #                                      :lower_breach_scale_increment => -1,
  #                                      :upper_threshold => 60,
  #                                      :upper_breach_scale_increment => 1,
  #                                      :breach_duration => 300 )
  #
  # Describe scaling activity:
  #
  #  as.incrementally_describe_scaling_activities('CentOS.5.1-c-array') #=> List of activities
  #
  # Describe the Auto Scaling group status:
  #
  #  as.describe_auto_scaling_groups('CentOS.5.1-c-array') #=> Current group status
  #
  class EmrInterface < RightAwsBase
    include RightAwsBaseInterface

    # Amazon AS API version being used
    API_VERSION       = '2009-03-31'
    DEFAULT_HOST      = 'elasticmapreduce.amazonaws.com'
    DEFAULT_PATH      = '/'
    DEFAULT_PROTOCOL  = 'https'
    DEFAULT_PORT      = 443

    @@bench = AwsBenchmarkingBlock.new
    def self.bench_xml
      @@bench.xml
    end
    def self.bench_service
      @@bench.service
    end

    # Create a new handle to an CSLS account. All handles share the same per process or per thread
    # HTTP connection to Amazon CSLS. Each handle is for a specific account. The params have the
    # following options:
    # * <tt>:endpoint_url</tt> a fully qualified url to Amazon API endpoint (this overwrites: :server, :port, :service, :protocol). Example: 'https://autoscaling.amazonaws.com/'
    # * <tt>:server</tt>: AS service host, default: DEFAULT_HOST
    # * <tt>:port</tt>: AS service port, default: DEFAULT_PORT
    # * <tt>:protocol</tt>: 'http' or 'https', default: DEFAULT_PROTOCOL
    # * <tt>:logger</tt>: for log messages, default: RAILS_DEFAULT_LOGGER else STDOUT
    # * <tt>:signature_version</tt>:  The signature version : '0','1' or '2'(default)
    # * <tt>:cache</tt>: true/false(default): describe_auto_scaling_groups
    #
    def initialize(aws_access_key_id=nil, aws_secret_access_key=nil, params={})
      init({ :name                => 'EMR',
             :default_host        => ENV['EMR_URL'] ? URI.parse(ENV['EMR_URL']).host   : DEFAULT_HOST,
             :default_port        => ENV['EMR_URL'] ? URI.parse(ENV['EMR_URL']).port   : DEFAULT_PORT,
             :default_service     => ENV['EMR_URL'] ? URI.parse(ENV['EMR_URL']).path   : DEFAULT_PATH,
             :default_protocol    => ENV['EMR_URL'] ? URI.parse(ENV['EMR_URL']).scheme : DEFAULT_PROTOCOL,
             :default_api_version => ENV['EMR_API_VERSION'] || API_VERSION },
           aws_access_key_id    || ENV['AWS_ACCESS_KEY_ID'] ,
           aws_secret_access_key|| ENV['AWS_SECRET_ACCESS_KEY'],
           params)
    end

    def generate_request(action, params={}) #:nodoc:
      generate_request_impl(:get, action, params )
    end

      # Sends request to Amazon and parses the response
      # Raises AwsError if any banana happened
    def request_info(request, parser)  #:nodoc:
      request_info_impl(:aass_connection, @@bench, request, parser)
    end

    #-----------------------------------------------------------------
    #      Auto Scaling Groups
    #-----------------------------------------------------------------

    # Describe auto scaling groups.
    # Returns a full description of the AutoScalingGroups from the given list.
    # This includes all EC2 instances that are members of the group. If a list
    # of names is not provided, then the full details of all AutoScalingGroups
    # is returned. This style conforms to the EC2 DescribeInstances API behavior.
    #
    def run_job_flow(options={})
      request_hash = amazonize_run_job_flow(options)
      request_hash.update(amazonize_bootstrap_actions(options[:bootstrap_actions]))
      request_hash.update(amazonize_instance_groups(options[:instance_groups]))
      request_hash.update(amazonize_steps(options[:steps]))
      link = generate_request("RunJobFlow", request_hash)
      request_info(link, RunJobFlowParser.new(:logger => @logger))
    end

    EMR_INSTANCES_KEY_MAPPING = {                                                           # :nodoc:
      :additional_info => 'AdditionalInfo',
      :log_uri => 'LogUri',
      :name => 'Name',
      # JobFlowInstancesConfig
      :ec2_key_name          => 'Instances.Ec2KeyName',
      :hadoop_version => 'Instances.HadoopVersion',
      :instance_count        => 'Instances.InstanceCount',
      :keep_job_flow_alive_when_no_steps   => 'Instances.KeepJobFlowAliveWhenNoSteps',
      :master_instance_type  => 'Instances.MasterInstanceType',
      :slave_instance_type   => 'Instances.SlaveInstanceType',
      :termination_protected => 'Instances.TerminationProtected',
      # PlacementType
      :availability_zone     => 'Instances.Placement.AvailabilityZone',
    }

    def amazonize_run_job_flow(options) # :nodoc:
      result = {}
      unless options.right_blank?
        EMR_INSTANCES_KEY_MAPPING.each do |local_name, remote_name|
          value = options[local_name]
          result[remote_name] = value unless value.nil?
        end
      end
      result
    end

    BOOTSTRAP_ACTION_KEY_MAPPING = {                                                           # :nodoc:
      :name => 'Name',
      # ScriptBootstrapActionConfig
      :args => 'Args',
      :path => 'Path',
    }

    def amazonize_bootstrap_actions(bootstrap_actions, key = 'BootstrapActions.member') # :nodoc:
      result = {}
      unless bootstrap_actions.right_blank?
        bootstrap_actions.each_with_index do |item, index|
          mapping.each do |local_name, remote_name|
            value = item[local_name]
            case local_name
            when :args
              result.update(amazonize_list("#{key}.#{index+1}.#{remote_name}", value))
            else
              next if value.nil?
              result["#{key}.#{index+1}.#{remote_name}"] = value
            end
          end
        end
      end
      result
    end

    INSTANCE_GROUP_KEY_MAPPING = {                                                           # :nodoc:
      :bid_price => 'BidPrice',
      :instance_count => 'InstanceCount',
      :instance_role => 'InstanceRole',
      :instance_type => 'InstanceType',
      :market => 'Market',
      :name => 'Name',
    }

    def amazonize_instance_groups(hash, key = 'Instances.InstanceGroups') # :nodoc:
      result = {}
      unless hash.right_blank?
        mapping.each do |local_name, remote_name|
          value = hash[local_name]
          case local_name
          when :instance_groups
            result.update(amazonize_list_with_key_mapping("#{key}.#{remote_name}", INSTANCE_GROUP_KEY_MAPPING, value))
          else
            next if value.nil?
            result["#{key}.#{remote_name}"] = value
          end
        end
      end
      result
    end

    STEP_CONFIG_KEY_MAPPING = {                                                           # :nodoc:
      :action_on_failure => 'ActionOnFailure',
      :name => 'Name',
      # HadoopJarStepConfig
      :args => 'HadoopJarStep.Args',
      :jar => 'HadoopJarStep.Jar',
      :main_class => 'HadoopJarStep.MainClass',
      :properties => 'HadoopJarStep.Properties',
    }
    
    KEY_VALUE_KEY_MAPPINGS = {
      :key => 'Key',
      :value => 'Value',
    }

    def amazonize_steps(steps, key = 'Steps.member') # :nodoc:
      result = {}
      unless steps.right_blank?
        steps.each_with_index do |item, index|
          STEP_CONFIG_KEY_MAPPING.each do |local_name, remote_name|
            value = item[local_name]
            case local_name
            when :args
              result.update(amazonize_list("#{key}.#{index+1}.#{remote_name}.member", value))
            when :properties
              next if value.right_blank?
              list = value.inject([]) do |l, (k, v)|
                l << {:key => k, :value => v}
              end
              result.update(amazonize_list_with_key_mappings("#{key}.#{index+1}.#{remote_name}", KEY_VALUE_KEY_MAPPINGS, list))
            else
              next if value.nil?
              result["#{key}.#{index+1}.#{remote_name}"] = value
            end
          end
        end
      end
      result
    end

    # Creates a new auto scaling group with the specified name.
    # Returns +true+ or raises an exception.
    #
    # Options: +:min_size+, +:max_size+, +:cooldown+, +:load_balancer_names+
    #
    #  as.create_auto_scaling_group('CentOS.5.1-c-array', 'CentOS.5.1-c', 'us-east-1c',
    #                               :min_size => 2,
    #                               :max_size => 5)  #=> true
    #
    # Amazon's notice: Constraints: Restricted to one Availability Zone
    def describe_job_flows(*job_flow_ids_and_options)
      job_flow_ids, options = AwsUtils::split_items_and_params(job_flow_ids_and_options)
      # merge job flow ids passed in as arguments and in options
      unless job_flow_ids.empty?
        # do not modify passed in options
        options = options.dup
        if job_flow_ids_in_options = options[:job_flow_ids]
          # allow the same ids to be passed in either location;
          # remove duplicates
          options[:job_flow_ids] = (job_flow_ids_in_options + job_flow_ids).uniq
        else
          options[:job_flow_ids] = job_flow_ids
        end
      end
      request_hash = {}
      unless (job_flow_ids = options[:job_flow_ids]).right_blank?
        request_hash.update(amazonize_list("JobFlowIds.member", job_flow_ids))
      end
      unless (job_flow_states = options[:job_flow_states]).right_blank?
        request_hash = amazonize_list("JobFlowStates.member", job_flow_states)
      end
      link = generate_request("DescribeJobFlows", request_hash)
      request_cache_or_info(:describe_job_flows, link,  DescribeJobFlowsParser, @@bench, nil)
    end

    # Deletes all configuration for this auto scaling group and also deletes the group.
    # Returns +true+ or raises an exception.
    #
    def terminate_job_flows(*job_flow_ids)
      link = generate_request("TerminateJobFlows", amazonize_list('JobFlowIds.member', job_flow_ids))
      request_info(link, RequestIdParser.new(:logger => @logger))
    end

    # Adjusts the desired size of the Capacity Group by using scaling actions, as necessary. When
    # adjusting the size of the group downward, it is not possible to define which EC2 instances will be
    # terminated. This also applies to any auto-scaling decisions that might result in the termination of
    # instances.
    #
    # Returns +true+ or raises an exception.
    #
    #  as.set_desired_capacity('CentOS.5.1-c',3) #=> 3
    #
    def set_termination_protection(*job_flow_ids_and_options)
      job_flow_ids, options = AwsUtils::split_items_and_params(job_flow_ids_and_options)
      request_hash = amazonize_list('JobFlowIds.member', job_flow_ids)
      request_hash['TerminationProtected'] = case value = options[:termination_protected]
      when true
        'true'
      when false
        'false'
      when nil
        # default is to protect
        'true'
      else
        # pass value through
        value
      end
      link = generate_request("SetTerminationProtection", request_hash)
      request_info(link, RequestIdParser.new(:logger => @logger))
    end

    # Updates the configuration for the given AutoScalingGroup. If MaxSize is lower than the current size,
    # then there will be an implicit call to SetDesiredCapacity to set the group to the new MaxSize. The
    # same is true for MinSize there will also be an implicit call to SetDesiredCapacity. All optional
    # parameters are left unchanged if not passed in the request.
    #
    # The new settings are registered upon the completion of this call. Any launch configuration settings
    # will take effect on any triggers after this call returns. However, triggers that are currently in
    # progress can not be affected. See key term Trigger.
    # 
    # Returns +true+ or raises an exception.
    #
    # Options: +:launch_configuration_name+, +:min_size+, +:max_size+, +:cooldown+, +:availability_zones+.
    # (Amazon's notice: +:availability_zones+ is reserved for future use.)
    #
    #  as.update_auto_scaling_group('CentOS.5.1-c', :min_size => 1, :max_size => 4) #=> true
    #
    def add_job_flow_steps(job_flow_id, *steps)
      request_hash = amazonize_steps(steps)
      request_hash['JobFlowId'] = job_flow_id
      link = generate_request("AddJobFlowSteps", request_hash)
      request_info(link, RequestIdParser.new(:logger => @logger))
    end

    #-----------------------------------------------------------------
    #      Scaling Activities
    #-----------------------------------------------------------------

    # Describe all Scaling Activities.
    #
    #  describe_scaling_activities('CentOS.5.1-c-array') #=>
    #        [{:cause=>
    #            "At 2009-05-28 10:11:35Z trigger kd.tr.1 breached high threshold value for
    #             CPUUtilization, 10.0, adjusting the desired capacity from 1 to 2.  At 2009-05-28 10:11:35Z
    #             a breaching trigger explicitly set group desired capacity changing the desired capacity
    #             from 1 to 2.  At 2009-05-28 10:11:40Z an instance was started in response to a difference
    #             between desired and actual capacity, increasing the capacity from 1 to 2.",
    #          :activity_id=>"067c9abb-f8a7-4cf8-8f3c-dc6f280457c4",
    #          :progress=>0,
    #          :description=>"Launching a new EC2 instance",
    #          :status_code=>"InProgress",
    #          :start_time=>Thu May 28 10:11:40 UTC 2009},
    #         {:end_time=>Thu May 28 09:35:23 UTC 2009,
    #          :cause=>
    #            "At 2009-05-28 09:31:21Z a user request created an AutoScalingGroup changing the desired
    #             capacity from 0 to 1.  At 2009-05-28 09:32:35Z an instance was started in response to a
    #             difference between desired and actual capacity, increasing the capacity from 0 to 1.",
    #          :activity_id=>"90d506ba-1b75-4d29-8739-0a75b1ba8030",
    #          :progress=>100,
    #          :description=>"Launching a new EC2 instance",
    #          :status_code=>"Successful",
    #          :start_time=>Thu May 28 09:32:35 UTC 2009}]}
    #
    def add_instance_groups(job_flow_id, *instance_groups)
      request_hash = amazonize_instance_groups(instance_groups)
      request_hash['JobFlowId'] = job_flow_id
      link = generate_request("AddInstanceGroups", request_hash)
      request_info(link, AddInstanceGroupParser.new(:logger => @logger))
    end
    
    INSTANCE_GROUP_KEY_MAPPINGS = {
      :instance_group_id => 'InstanceGroupId',
      :instance_count => 'InstanceCount',
    }

    # Modifies instance groups.
    #
    # The only modifiable parameter is instance count.
    #
    # An instance group may only be modified when the job flow is running
    # or waiting. Additionally, hadoop 0.20 is required to resize job flows.
    #
    #  # general syntax
    #  emr.modify_instance_groups(
    #    {:instance_group_id => 'ig-P2OPM2L9ZQ4P', :instance_count => 5},
    #    {:instance_group_id => 'ig-J82ML0M94A7E', :instance_count => 1}
    #  ) #=> true
    #
    #  # shortcut syntax
    #  emr.modify_instance_groups('ig-P2OPM2L9ZQ4P', 5) #=> true
    #
    # Shortcut syntax supports modifying only one instance group at a time.
    #
    def modify_instance_groups(*args)
      unless args.first.is_a?(Hash)
        if args.length != 2
          raise ArgumentError, "Must be given two arguments if arguments are not hashes"
        end
        args = [{:instance_group_id => args.first, :instance_count => args.last}]
      end
      request_hash = amazonize_list_with_key_mapping('InstanceGroups.member', INSTANCE_GROUP_KEY_MAPPINGS, args)
      link = generate_request("ModifyInstanceGroups", request_hash)
      request_info(link, RequestIdParser.new(:logger => @logger))
    end

    #-----------------------------------------------------------------
    #      PARSERS: Auto Scaling Groups
    #-----------------------------------------------------------------

    class RunJobFlowParser < RightAWSParser #:nodoc:
      def tagend(name)
        case name
        when 'JobFlowId'             then @result              = @text
        end
      end
      def reset
        @result = nil
      end
    end

    #-----------------------------------------------------------------
    #      PARSERS: Job Flows
    #-----------------------------------------------------------------

    class DescribeJobFlowsParser < RightAWSParser #:nodoc:
      def tagstart(name, attributes)
        case full_tag_name
        when %r{/JobFlows/member$}
          @item = { :instance_groups => [],
                    :steps       => [],
                    :bootstrap_actions => [] }
        when %r{/BootstrapActionConfig$}
          @bootstrap_action = {}
        when %r{/InstanceGroups/member$}
          @instance_group = {}
        when %r{/Steps/member$}
          @step = { :args => [],
                    :properties => {} }
        end
      end
      def tagend(name)
        case full_tag_name
        when %r{/BootstrapActionConfig} # no trailing $
          case name
          when 'Name'
            @bootstrap_action[:name] = @text
          when 'ScriptBootstrapAction'
            @bootstrap_action[:script_bootstrap_action] = @text
          when 'BootstrapActionConfig'
            @step[:bootstrap_actions] << @bootstrap_action
          end
        when %r{/InstanceGroups/member} # no trailing $
          case name
          when 'BidPrice' then @instance_group[:bid_price] = @text
          when 'CreationDateTime' then @instance_group[:creation_date_time] = @text
          when 'EndDateTime' then @instance_group[:end_date_time] = @text
          when 'InstanceGroupId' then @instance_group[:instance_group_id] = @text
          when 'InstanceRequestCount' then @instance_group[:instance_request_count] = @text.to_i
          when 'InstanceRole' then @instance_group[:instance_role] = @text
          when 'InstanceRunningCount' then @instance_group[:instance_running_count] = @text.to_i
          when 'InstanceType' then @instance_group[:instance_type] = @text
          when 'LastStateChangeReason' then @instance_group[:last_state_change_reason] = @text
          when 'Market' then @instance_group[:market] = @text
          when 'Name' then @instance_group[:name] = @text
          when 'ReadyDateTime' then @instance_group[:ready_date_time] = @text
          when 'StartDateTime' then @instance_group[:start_date_time] = @text
          when 'State' then @instance_group[:state] = @text
          when 'member'              then @item[:instance_groups]        << @instance_group
          end
        when %r{/Steps/member/StepConfig/HadoopJarStepConfig/Args/member}
          @steps[:args] << @text
        when %r{/Steps/member/StepConfig/HadoopJarStepConfig/Properties}
          case name
          when 'Key'
            @key = @text
          when 'Value'
            @steps[:properties][@key] = @text
          end
        when %r{/Steps/member$}
          @item[:steps]        << @step
        when %r{/Steps/member} # no trailing $
          case name
          # ExecutionStatusDetail
          when 'CreationDateTime' then @step[:creation_date_time] = @text
          when 'EndDateTime' then @step[:end_date_time] = @text
          when 'LastStateChangeReason' then @step[:last_state_change_reason] = @text
          when 'StartDateTime' then @step[:start_date_time] = @text
          when 'State' then @step[:state] = @text
          # StepConfig
          when 'ActionOnFailure' then @step[:action_on_failure] = @text
          when 'Name' then @step[:name] = @text
          # HadoopJarStepConfig
          when 'Jar' then @step[:jar] = @text
          when 'MainClass' then @step[:main_class] = @text
          end
        when %r{/JobFlows/member$}
          @result[:job_flows] << @item
        else
          case name
          when 'AmiVersion'             then @item[:ami_version]              = @text
          when 'JobFlowId'            then @item[:job_flow_id]             = @text
          when 'LogUri'                 then @item[:log_uri]                  = @text
          when 'Name'                 then @item[:name]                  = @text
          
          # JobFlowExecutionStatusDetail
          when 'CreationDateTime'                 then @item[:creation_date_time]                  = @text
          when 'EndDateTime'                 then @item[:end_date_time]                  = @text
          when 'LastStateChangeReason'                 then @item[:last_state_change_reason]                  = @text
          when 'ReadyDateTime'                 then @item[:ready_date_time]                  = @text
          when 'StartDateTime'                 then @item[:start_date_time]                  = @text
          when 'State'                 then @item[:state]                  = @text
          
          # JobFlowInstancesDetail
          when 'Ec2KeyName'                 then @item[:ec2_key_name]                  = @text
          when 'HadoopVersion'                 then @item[:hadoop_version]                  = @text
          when 'InstanceCount'                 then @item[:instance_count]                  = @text.to_i
          when 'KeepJobFlowAliveWhenNoSteps'                 then @item[:keep_job_flow_alive_when_no_steps]                  = case @text when 'true' then true when 'false' then false else @text end
          when 'MasterInstanceId'                 then @item[:master_instance_id]                  = @text
          when 'MasterInstanceType'                 then @item[:master_instance_type]                  = @text
          when 'MasterPublicDnsName'                 then @item[:master_public_dns_name]                  = @text
          when 'NormalizedInstanceHours'                 then @item[:normalized_instance_hours]                  = @text.to_i
          # Placement
          when 'AvailabilityZone'                 then @item[:availability_zone]                  = @text
          when 'SlaveInstanceType'                 then @item[:slave_instance_type]                  = @text
          when 'TerminationProtected'                 then @item[:termination_protected]                  = case @text when 'true' then true when 'false' then false else @text end
          end
        end
      end
      def reset
        @result = { :job_flows => []}
      end
    end

    class RequestIdParser < RightAWSParser #:nodoc:
      def tagend(name)
        case name
        when 'RequestId' then @result = true
        end
      end
      def reset
        @result = nil
      end
    end

    #-----------------------------------------------------------------
    #      PARSERS: Triggers
    #-----------------------------------------------------------------

    class AddInstanceGroupsParser < RightAWSParser #:nodoc:
      def tagend(name)
        case name
        when 'InstanceGroupId' then @result << @text
        end
      end
      def reset
        @result = []
      end
    end
  end

end
