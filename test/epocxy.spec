%% -*- mode: erlang -*-
%% -*- tab-width: 4;erlang-indent-level: 4;indent-tabs-mode: nil -*-
%% ex: ts=4 sw=4 et

%%%------------------------------------------------------------------------------
%%% @copyright (c) 2015-2016, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com>
%%% @reference 2015-2016 Development sponsored by TigerText, Inc. [http://tigertext.com/]
%%% @reference The license is based on the template for Modified BSD from
%%%   <a href="http://opensource.org/licenses/BSD-3-Clause">OSI</a>
%%% @end
%%%------------------------------------------------------------------------------
{alias, epocxy, "./epocxy/"}.
{include, ["../include"]}.
{logdir, "./epocxy/logs/"}.
{cover, "./epocxy.coverspec"}.
{suites, epocxy, [
                  batch_feeder_SUITE,
                  ets_buffer_SUITE,
                  cxy_ctl_SUITE,
                  cxy_cache_SUITE,

                  %% cxy_fount has multiple components
                  cxy_regulator_SUITE,
                  cxy_fount_SUITE
                 ]}.
