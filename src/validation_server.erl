%%% validation_server.erl 
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%  @author Chanaka Fernando <chanaka@globalwavenet.com>
%%%  @copyright 2015 Wavenet International (Pvt) Ltd.
%%%  @end
%%%  This computer program(s) is proprietary software and the intellectual
%%%  property of WAVENET INTERNATIONAL (PVT) LIMITED (hereinafter referred
%%%  to as "Wavenet").  Unless otherwise specified, all materials contained
%%%  in herein are copyrighted and may not be used except as provided in 
%%%  these terms and conditions or in the copyright notice (documents and
%%%  software) or other proprietary notice provided with, or attached to,
%%%  the software or relevant document, or is otherwise referenced as 
%%%  applicable to the software.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc This {@link //stdlib/gen_server. gen_server} behaviour callback
%%% 	module implements a ... server in the
%%% 	{@link //validation. validation} application.
%%%
-module(validation_server).
-copyright('Copyright (c) 2015 Wavenet International (Pvt) Ltd.').
-author('Chanaka Fernando <chanaka@globalwavenet.com>').

-behaviour(gen_server).

%% export the validation_server API
-export([send_response/4,
global_charging_blacklist/4,
global_subscription_blacklist/4,
service_group_blacklist/4,
service_blacklist/4,
service_group_whitelist/4,
service_whitelist/4,
device_blacklist/4,
partner_dnd/4,
service_details/4]).

%% export the callbacks needed for gen_server behaviour
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
			terminate/2, code_change/3]).

-record(state, {id :: binary(), talend_host :: string()}).

%%----------------------------------------------------------------------
%%  The validation_server API
%%----------------------------------------------------------------------

%%----------------------------------------------------------------------
%%  The validation_server gen_server callbacks
%%----------------------------------------------------------------------

-spec init(Args :: [term()]) ->
	{ok, State :: #state{}}
			| {ok, State :: #state{}, Timeout :: timeout()}
			| {stop, Reason :: term()} | ignore.
%% @doc Initialize the {@module} server.
%% @see //stdlib/gen_server:init/1
%% @private
%%
init([] = _Args) ->
	process_flag(trap_exit, true),
	erlang:start_timer(500, self(), fetch_topics),
	Id = os:system_time(),
	{ok, #state{id = integer_to_binary(Id, 16), talend_host = "localhost:8088"}}.

-spec handle_call(Request :: term(), From :: {pid(), Tag :: any()},
		State :: #state{}) ->
	{reply, Reply :: term(), NewState :: #state{}}
			| {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate}
			| {noreply, NewState :: #state{}}
			| {noreply, NewState :: #state{}, timeout() | hibernate}
			| {stop, Reason :: term(), Reply :: term(), NewState :: #state{}}
			| {stop, Reason :: term(), NewState :: #state{}}.
%% @doc Handle a request sent using {@link //stdlib/gen_server:call/2.
%% 	gen_server:call/2,3} or {@link //stdlib/gen_server:multi_call/2.
%% 	gen_server:multi_call/2,3,4}.
%% @see //stdlib/gen_server:handle_call/3
%% @private
%%
handle_call(_Request, _From, State) ->
	{stop, not_implemented, State}.

-spec handle_cast(Request :: term(), State :: #state{}) ->
	{noreply, NewState :: #state{}}
			| {noreply, NewState :: #state{}, timeout() | hibernate}
			| {stop, Reason :: term(), NewState :: #state{}}.
%% @doc Handle a request sent using {@link //stdlib/gen_server:cast/2.
%% 	gen_server:cast/2} or {@link //stdlib/gen_server:abcast/2.
%% 	gen_server:abcast/2,3}.
%% @see //stdlib/gen_server:handle_cast/2
%% @private
%%
handle_cast(stop, State) ->
	{stop, normal, State}.

-spec handle_info(Info :: timeout | term(), State::#state{}) ->
	{noreply, NewState :: #state{}}
			| {noreply, NewState :: #state{}, timeout() | hibernate}
			| {stop, Reason :: term(), NewState :: #state{}}.
%% @doc Handle a received message.
%% @see //stdlib/gen_server:handle_info/2
%% @private
%%
handle_info({timeout, _TimerRef, fetch_topics}, State) ->
	case (catch openapi_external_task_api:fetch_and_lock(#{},#{body =>#{<<"workerId">> => State#state.id, <<"maxTasks">> => 100, <<"usePriority">> => true, <<"topics">> => [
	#{<<"topicName">> => <<"service-details">>, <<"lockDuration">> => 10000, <<"variables">> => [<<"service_code">>]},
	#{<<"topicName">> => <<"partner-dnd">>, <<"lockDuration">> => 10000, <<"variables">> => [<<"partner_id">>, <<"msisdn">>]},
	#{<<"topicName">> => <<"device-blacklist">>, <<"lockDuration">> => 10000, <<"variables">> => [<<"service_id">>, <<"device_model_code">>]},
	#{<<"topicName">> => <<"service-whitelist">>, <<"lockDuration">> => 10000, <<"variables">> => [<<"service_id">>, <<"msisdn">>]},
	#{<<"topicName">> => <<"service-group-whitelist">>, <<"lockDuration">> => 10000, <<"variables">> => [<<"service_id">>, <<"msisdn">>]},
	#{<<"topicName">> => <<"service-blacklist">>, <<"lockDuration">> => 10000, <<"variables">> => [<<"service_id">>, <<"msisdn">>]},
	#{<<"topicName">> => <<"service-group-blacklist">>, <<"lockDuration">> => 10000, <<"variables">> => [<<"service_id">>, <<"msisdn">>]},
	#{<<"topicName">> => <<"global-subscription-blacklist">>, <<"lockDuration">> => 10000, <<"variables">> => [ <<"msisdn">>]},
	#{<<"topicName">> => <<"global-charging-blacklist">>, <<"lockDuration">> => 10000, <<"variables">> => [ <<"msisdn">>]},
	#{<<"topicName">> => <<"send-response">>, <<"lockDuration">> => 10000, <<"variables">> => [ <<"status">>]}
	]}})) of
		{ok, ProcessDataList, _} ->
			%io:fwrite("[~p|~p] ProcessDataList : ~p~n",[?MODULE, ?LINE, ProcessDataList]),
			distribute_tasks(ProcessDataList, State),
			erlang:start_timer(50, self(), fetch_topics);
		Error ->
			io:fwrite("openapi_external_task_api:fetch_and_lock Error : ~p~n",[Error]),
			erlang:start_timer(2000, self(), fetch_topics)
	end,
	{noreply, State};
handle_info(_Info, State) ->
	{stop, not_implemented, State}.

-spec terminate(Reason :: normal | shutdown | {shutdown, term()} | term(),
		State::#state{}) ->
	any().
%% @doc Cleanup and exit.
%% @see //stdlib/gen_server:terminate/3
%% @private
%%
terminate(_Reason, _State) ->
	ok.

-spec code_change(OldVsn :: term() | {down, term()}, State :: #state{},
		Extra :: term()) ->
	{ok, NewState :: #state{}} | {error, Reason :: term()}.
%% @doc Update internal state data during a release upgrade&#047;downgrade.
%% @see //stdlib/gen_server:code_change/3
%% @private
%%
code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%%----------------------------------------------------------------------
%%  internal functions
%%----------------------------------------------------------------------
distribute_tasks([], _State) ->
	ok;
distribute_tasks([ProcessData|Rest], State) ->
	TopicName = maps:get(topicName, ProcessData, []),
	BusinessKey = maps:get(businessKey, ProcessData, []),
	Id = maps:get(id, ProcessData, []),
	Variables = maps:get(variables, ProcessData, []),
	case TopicName of
		<<"service-details">> ->
			ServiceCode = maps:get(value, maps:get(service_code, Variables, #{value => []}),[]),
			spawn(?MODULE, service_details, [BusinessKey, Id, ServiceCode, State]);
		<<"partner-dnd">> ->
			PartnerId = maps:get(value, maps:get(partner_id, Variables, #{value => []}),[]),
			Msisdn = maps:get(value, maps:get(msisdn, Variables, #{value => []}),[]),
			spawn(?MODULE, partner_dnd, [BusinessKey, Id, {PartnerId, Msisdn}, State]);
		<<"device-blacklist">> ->
			ServiceId = maps:get(value, maps:get(service_id, Variables, #{value => []}),[]),
			DeviceModelCode = maps:get(value, maps:get(device_model_code, Variables, #{value => []}),[]),
			spawn(?MODULE, device_blacklist, [BusinessKey, Id, {ServiceId, DeviceModelCode}, State]);
		<<"service-whitelist">> ->
			ServiceId = maps:get(value, maps:get(service_id, Variables, #{value => []}),[]),
			Msisdn = maps:get(value, maps:get(msisdn, Variables, #{value => []}),[]),
			spawn(?MODULE, service_whitelist, [BusinessKey, Id, {ServiceId, Msisdn}, State]);
		<<"service-group-whitelist">> ->
			ServiceId = maps:get(value, maps:get(service_id, Variables, #{value => []}),[]),
			Msisdn = maps:get(value, maps:get(msisdn, Variables, #{value => []}),[]),
			spawn(?MODULE, service_group_whitelist, [BusinessKey, Id, {ServiceId, Msisdn}, State]);
		<<"service-blacklist">> ->
			ServiceId = maps:get(value, maps:get(service_id, Variables, #{value => []}),[]),
			Msisdn = maps:get(value, maps:get(msisdn, Variables, #{value => []}),[]),
			spawn(?MODULE, service_blacklist, [BusinessKey, Id, {ServiceId, Msisdn}, State]);
		<<"service-group-blacklist">> ->
			ServiceId = maps:get(value, maps:get(service_id, Variables, #{value => []}),[]),
			Msisdn = maps:get(value, maps:get(msisdn, Variables, #{value => []}),[]),
			spawn(?MODULE, service_group_blacklist, [BusinessKey, Id, {ServiceId, Msisdn}, State]);
		<<"global-subscription-blacklist">> ->
			Msisdn = maps:get(value, maps:get(msisdn, Variables, #{value => []}),[]),
			spawn(?MODULE, global_subscription_blacklist, [BusinessKey, Id, Msisdn, State]);
		<<"global-charging-blacklist">> ->
			Msisdn = maps:get(value, maps:get(msisdn, Variables, #{value => []}),[]),
			spawn(?MODULE, global_charging_blacklist, [BusinessKey, Id, Msisdn, State]);
		<<"send-response">> ->
			Status = maps:get(value, maps:get(status, Variables, #{value => []}),[]),
			spawn(?MODULE, send_response, [BusinessKey, Id, Status, State]);
		OtherTopic ->
		   io:fwrite("[~p|~p] distribute_tasks OtherTopic : ~p : ProcessData : ~p~n", [?MODULE, ?LINE, OtherTopic, ProcessData])
	end,
distribute_tasks(Rest, State).

service_details(BusinessKey, Id, ServiceCode, State) ->
	io:fwrite("[~p|~p] service_details Id : ~p : BusinessKey: ~p : ServiceCode ~p~n", [?MODULE, ?LINE, Id, BusinessKey, ServiceCode]),
	{ok, ResBody, _ } = swagger_default_api:service_details(#{}, #{params => #{service_code => ServiceCode}, cfg => #{host => State#state.talend_host}}), 
	ServiceDetails = maps:get(service_details, maps:get(services, ResBody,#{}), #{}),
	io:fwrite("[~p|~p] ~p~n",[?MODULE, ?LINE, ServiceDetails]),
	if
		(ServiceDetails == <<>>) or (ServiceDetails == #{}) ->
			openapi_external_task_api:complete_external_task_resource(#{}, Id, #{body =>#{<<"workerId">> => State#state.id, 
				<<"variables">> => #{
				<<"managed">> => #{<<"value">> => false},
				<<"service_type">> => #{<<"value">> => <<"on-demand">>}
				}}});
		true ->
			ServiceId = maps:get(id, ServiceDetails, 0),
			ServiceCode = maps:get(service_code, ServiceDetails, <<"Unknown">>),
			PartnerId = maps:get(partner_id, ServiceDetails, 0),
			Name = maps:get(name, ServiceDetails, <<"Unknown">>),
			Managed = maps:get(managed, ServiceDetails, false),
			GlobalServiceWhitelist = maps:get(global_service_whitelist, ServiceDetails, false),
			EnableServiceWhitelist = maps:get(enable_service_whitelist, ServiceDetails, fasle),
			GlobaleServiceBlacklist = maps:get(global_service_blacklist, ServiceDetails, fasle),
			EnableSeriviceBlacklist = maps:get(enable_service_blacklist, ServiceDetails, fasle),
			EnableGlobalSubscriptionBlacklist = maps:get(enable_global_subscription_blacklist, ServiceDetails, fasle),
			EnableGlobalChargingBlacklist = maps:get(enable_global_charging_blacklist, ServiceDetails, fasle),
			ServiceType = maps:get(service_type, ServiceDetails, <<"on-demand">>),
			% #{service_details =>
			%    #{enable_global_charging_blacklist => false,
			%      enable_global_subscription_blacklist => false,
			%      enable_service_blacklist => false,
			%      enable_service_whitelist => false,
			%      global_service_blacklist => false,
			%      global_service_whitelist => false,id => 7,managed => false,
			%      name => <<"service106">>,partner_id => 10,
			%      service_code => <<"wn106">>,
			%      service_type => <<"on-demand">>}}},
			openapi_external_task_api:complete_external_task_resource(#{}, Id, #{body =>#{<<"workerId">> => State#state.id, 
			<<"variables">> => #{
			<<"service_id">> => #{<<"value">> => ServiceId}, 
			<<"service_code">> => #{<<"value">> => ServiceCode},
			<<"partner_id">> => #{<<"value">> => PartnerId},
			<<"name">> => #{<<"value">> => Name},
			<<"managed">> => #{<<"value">> => Managed},
			<<"global_service_whitelist">> => #{<<"value">> => GlobalServiceWhitelist},
			<<"enable_service_whitelist">> => #{<<"value">> => EnableServiceWhitelist},
			<<"global_service_blacklist">> => #{<<"value">> => GlobaleServiceBlacklist},
			<<"enable_service_blacklist">> => #{<<"value">> => EnableSeriviceBlacklist},
			<<"enable_global_subscription_blacklist">> => #{<<"value">> => EnableGlobalSubscriptionBlacklist},
			<<"enable_global_charging_blacklist">> => #{<<"value">> => EnableGlobalChargingBlacklist},
			<<"service_type">> => #{<<"value">> => ServiceType}
			}}})
	end.
	
partner_dnd(BusinessKey, Id, {PartnerId, Msisdn}, State) ->
	io:fwrite("[~p|~p] partner_dnd Id : ~p : BusinessKey: ~p : {PartnerId, Msisdn} ~p~n", [?MODULE, ?LINE, Id, BusinessKey, {PartnerId, Msisdn}]),
	{ok, ResBody, _ } = swagger_default_api:partner_dnd(#{}, #{params => #{partner_id => PartnerId, msisdn => Msisdn}, cfg => #{host => State#state.talend_host}}), % {partner_dnd => #{status => true}}
	Status = maps:get(status, maps:get(partner_dnd, ResBody,#{}), false),
	openapi_external_task_api:complete_external_task_resource(#{}, Id, #{body =>#{<<"workerId">> => State#state.id, 
	<<"variables">> => #{<<"partner_dnd">> => #{<<"value">> => Status}}}}).
	
device_blacklist(BusinessKey, Id, {ServiceId, DeviceModelCode}, State) ->
	io:fwrite("[~p|~p] device_blacklist Id : ~p : BusinessKey: ~p : {ServiceId, DeviceId} ~p~n", [?MODULE, ?LINE, Id, BusinessKey, {ServiceId, DeviceModelCode}]),
	{ok, ResBody, _ } = swagger_default_api:device_blacklist(#{}, #{params => #{service_id => ServiceId, model_code => DeviceModelCode}, cfg => #{host => State#state.talend_host}}), % {device_blacklist => #{status => true}}
	Status = maps:get(status, maps:get(device_blacklist, ResBody,#{}), false),
	openapi_external_task_api:complete_external_task_resource(#{}, Id, #{body =>#{<<"workerId">> => State#state.id, 
	<<"variables">> => #{<<"device_blacklist">> => #{<<"value">> => Status}}}}).
	
service_whitelist(BusinessKey, Id, {ServiceId, Msisdn}, State) ->
	io:fwrite("[~p|~p] service_whitelist Id : ~p : BusinessKey: ~p : {ServiceId, Msisdn} ~p~n", [?MODULE, ?LINE, Id, BusinessKey, {ServiceId, Msisdn}]),
	{ok, ResBody, _ } = swagger_default_api:service_whitelist(#{}, #{params => #{service_id => ServiceId,msisdn => Msisdn}, cfg => #{host => State#state.talend_host}}), % #{service_whitelist => #{status => false}}
	Status = maps:get(status, maps:get(service_whitelist, ResBody,#{}), false),
	Value = 
	if 
		(Status == true) -> <<"service whitelist">>;
		true -> <<"none">>
	end,
	openapi_external_task_api:complete_external_task_resource(#{}, Id, #{body =>#{<<"workerId">> => State#state.id, 
	<<"variables">> => #{<<"listed">> => #{<<"value">> => Value}}}}).	

service_group_whitelist(BusinessKey, Id, {ServiceId, Msisdn}, State) ->
	io:fwrite("[~p|~p] service_group_whitelist Id : ~p : BusinessKey: ~p : {ServiceId, Msisdn} ~p~n", [?MODULE, ?LINE, Id, BusinessKey, {ServiceId, Msisdn}]),
	{ok, ResBody, _ } = swagger_default_api:service_group_whitelist(#{}, #{params => #{service_id => ServiceId,msisdn => Msisdn}, cfg => #{host => State#state.talend_host}}), % #{service_group_whitelist => #{status => false}}
	Status = maps:get(status, maps:get(service_group_whitelist, ResBody,#{}), false),
	Value = 
	if 
		(Status == true) -> <<"service whitelist">>;
		true -> <<"none">>
	end,
	openapi_external_task_api:complete_external_task_resource(#{}, Id, #{body =>#{<<"workerId">> => State#state.id, 
	<<"variables">> => #{<<"listed">> => #{<<"value">> => Value}}}}).

service_blacklist(BusinessKey, Id, {ServiceId, Msisdn}, State) ->
	io:fwrite("[~p|~p] service_blacklist Id : ~p : BusinessKey: ~p : {ServiceId, Msisdn} ~p~n", [?MODULE, ?LINE, Id, BusinessKey, {ServiceId, Msisdn}]),
	{ok, ResBody, _ } = swagger_default_api:service_blacklist(#{}, #{params => #{service_id => ServiceId,msisdn => Msisdn}, cfg => #{host => State#state.talend_host}}), % #{service_blacklist => #{status => false}}
	Status = maps:get(status, maps:get(service_blacklist, ResBody,#{}), false),
	Value = 
	if 
		(Status == true) -> <<"service blacklist">>;
		true -> <<"none">>
	end,
	openapi_external_task_api:complete_external_task_resource(#{}, Id, #{body =>#{<<"workerId">> => State#state.id, 
	<<"variables">> => #{<<"listed">> => #{<<"value">> => Value}}}}).

service_group_blacklist(BusinessKey, Id, {ServiceId, Msisdn}, State) ->
	io:fwrite("[~p|~p] service_group_blacklist Id : ~p : BusinessKey: ~p : {ServiceId, Msisdn} ~p~n", [?MODULE, ?LINE, Id, BusinessKey, {ServiceId, Msisdn}]),
	{ok, ResBody, _ } = swagger_default_api:service_group_blacklist(#{}, #{params => #{service_id => ServiceId,msisdn => Msisdn}, cfg => #{host => State#state.talend_host}}), % #{service_group_blacklist => #{status => false}}
	Status = maps:get(status, maps:get(service_group_blacklist, ResBody,#{}), false),
	Value = 
	if 
		(Status == true) -> <<"service blacklist">>;
		true -> <<"none">>
	end,
	openapi_external_task_api:complete_external_task_resource(#{}, Id, #{body =>#{<<"workerId">> => State#state.id, 
	<<"variables">> => #{<<"listed">> => #{<<"value">> => Value}}}}).

global_subscription_blacklist(BusinessKey, Id, Msisdn, State) ->
	io:fwrite("[~p|~p] global_subscription_blacklist Id : ~p : BusinessKey: ~p : Msisdn ~p~n", [?MODULE, ?LINE, Id, BusinessKey, Msisdn]),
	{ok, ResBody, _ } = swagger_default_api:service_global_subscription_blacklist(#{}, #{params => #{msisdn => Msisdn}, cfg => #{host => State#state.talend_host}}), % #{global_subscription_blacklist => #{status => true}}
	Status = maps:get(status, maps:get(global_subscription_blacklist, ResBody,#{}), false),
	Value = 
	if 
		(Status == true) -> <<"global subscription blacklist">>;
		true -> <<"none">>
	end,
	openapi_external_task_api:complete_external_task_resource(#{}, Id, #{body =>#{<<"workerId">> => State#state.id, 
	<<"variables">> => #{<<"listed">> => #{<<"value">> => Value}}}}).
	
global_charging_blacklist(BusinessKey, Id, Msisdn, State) ->
	io:fwrite("[~p|~p] global_charging_blacklist Id : ~p : BusinessKey: ~p : Msisdn ~p~n", [?MODULE, ?LINE, Id, BusinessKey, Msisdn]),
	{ok, ResBody, _ } = swagger_default_api:service_global_charging_blacklist(#{}, #{params => #{msisdn => Msisdn}, cfg => #{host => State#state.talend_host}}), % #{global_charging_blacklist => #{status => true}}
	Status = maps:get(status, maps:get(global_charging_blacklist, ResBody,#{}), false),
	Value = 
	if 
		(Status == true) -> <<"global charging blacklist">>;
		true -> <<"none">>
	end,
	openapi_external_task_api:complete_external_task_resource(#{}, Id, #{body =>#{<<"workerId">> => State#state.id, 
	<<"variables">> => #{<<"listed">> => #{<<"value">> => Value}}}}).
	
send_response(BusinessKey, Id, Status, State) ->
	io:fwrite("[~p|~p] response Id: ~p : BusinessKey : ~p : Status ~p~n", [?MODULE, ?LINE, Id, BusinessKey, Status]),
	%%
	catch swagger_default_logic_handler:handle_response('Response', {BusinessKey, Status}),
	%%
	openapi_external_task_api:complete_external_task_resource(#{}, Id, #{body =>#{<<"workerId">> => State#state.id}}).