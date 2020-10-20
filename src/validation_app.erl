%%% validation_app.erl 
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
%%% @doc This {@link //stdlib/application. application} behaviour callback
%%% 	module starts and stops the
%%% 	{@link //validation. validation} application.
%%%
-module(validation_app).
-copyright('Copyright (c) 2015 Wavenet International (Pvt) Ltd.').
-author('Chanaka Fernando <chanaka@globalwavenet.com>').

-behaviour(application).

%% callbacks needed for application behaviour
-export([start/2, stop/1, config_change/3]).
%% optional callbacks for application behaviour
-export([prep_stop/1, start_phase/3]).

-record(state, {}).

%%----------------------------------------------------------------------
%%  The validation_app aplication callbacks
%%----------------------------------------------------------------------

-type start_type() :: normal | {takeover, node()} | {failover, node()}.
-spec start(StartType :: start_type(), StartArgs :: term()) ->
	{'ok', pid()} | {'ok', pid(), State :: #state{}}
			| {'error', Reason :: term()}.
%% @doc Starts the application processes.
%% @see //kernel/application:start/1
%% @see //kernel/application:start/2
%%
start(normal = _StartType, _Args) ->
	case supervisor:start_link(validation_sup, []) of
		{ok, Sup} ->
			{ok, Sup};
		{error, Reason} ->
			error_logger:error_report(["validation application failed to start",
					{reason, Reason}, {module, ?MODULE}]),
			{error, Reason}
	end.

-spec start_phase(Phase :: atom(), StartType :: start_type(),
		PhaseArgs :: term()) -> ok | {error, Reason :: term()}.
%% @doc Called for each start phase in the application and included
%% 	applications.
%% @see //kernel/app
%%
start_phase(_Phase, _StartType, _PhaseArgs) ->
	ok.

-spec prep_stop(State :: #state{}) -> #state{}.
%% @doc Called when the application is about to be shut down,
%% 	before any processes are terminated.
%% @see //kernel/application:stop/1
%%
prep_stop(State) ->
	State.

-spec stop(State :: #state{}) -> any().
%% @doc Called after the application has stopped to clean up.
%%
stop(_State) ->
	ok.

-spec config_change(Changed :: [{Par :: atom(), Val :: atom()}],
		New :: [{Par :: atom(), Val :: atom()}],
		Removed :: [Par :: atom()]) -> ok.
%% @doc Called after a code  replacement, if there are any 
%% 	changes to the configuration  parameters.
%%
config_change(_Changed, _New, _Removed) ->
	ok.

%%----------------------------------------------------------------------
%%  internal functions
%%----------------------------------------------------------------------

