%%% validation_sup.erl 
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
%%% @docfile "supervision.edoc"
%%%
-module(validation_sup).
-copyright('Copyright (c) 2015 Wavenet International (Pvt) Ltd.').
-author('Chanaka Fernando <chanaka@globalwavenet.com>').

-behaviour(supervisor).

%% export the callback needed for supervisor behaviour
-export([init/1]).

%%----------------------------------------------------------------------
%%  The supervisor callback
%%----------------------------------------------------------------------

-spec init(Args :: [term()]) ->
	{ok, {{supervisor:strategy(), non_neg_integer(), pos_integer()},
			[supervisor:child_spec()]}} | ignore.
%% @doc Initialize the {@module} supervisor.
%% @see //stdlib/supervisor:init/1
%% @private
%%
init([] = _Args) ->
	ChildSpecs = [server(validation_server, [])],
	{ok, {{one_for_one, 10, 60}, ChildSpecs}}.

%%----------------------------------------------------------------------
%%  internal functions
%%----------------------------------------------------------------------

% -spec supervisor(StartMod :: atom(), Args :: [term()]) ->
	% supervisor:child_spec().
%% @doc Build a supervisor child specification for a
%% 	{@link //stdlib/supervisor. supervisor} behaviour.
%% @private
%%
% supervisor(StartMod, Args) ->
	% StartArgs = [{local, StartMod}, StartMod, Args],
	% StartFunc = {supervisor, start_link, StartArgs},
	% {StartMod, StartFunc, permanent, infinity, supervisor, [StartMod]}.

-spec server(StartMod :: atom(), Args :: [term()]) ->
	supervisor:child_spec().
%% @doc Build a supervisor child specification for a
%% 	{@link //stdlib/gen_server. gen_server} behaviour.
%% @private
%%
server(StartMod, Args) ->
	StartArgs = [StartMod, Args, []],
	StartFunc = {gen_server, start_link, StartArgs},
	{StartMod, StartFunc, permanent, 4000, worker, [StartMod]}.

