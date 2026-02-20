import 'package:flutter/material.dart';
import 'package:provider/provider.dart';
import 'package:recon/clients/session_client.dart';
import 'package:recon/models/session.dart';
import 'package:recon/widgets/friends/friends_list.dart';
import 'package:recon/widgets/friends/friends_list_app_bar.dart';
import 'package:recon/widgets/inventory/inventory_browser.dart';
import 'package:recon/widgets/inventory/inventory_browser_app_bar.dart';
import 'package:recon/widgets/sessions/in_world_now_page.dart';
import 'package:recon/widgets/sessions/session_list.dart';
import 'package:recon/widgets/sessions/session_list_app_bar.dart';
import 'package:recon/widgets/settings_app_bar.dart';
import 'package:recon/widgets/settings_page.dart';
import 'package:recon/widgets/worlds/world_list.dart';
import 'package:recon/widgets/worlds/world_list_app_bar.dart';

class Home extends StatefulWidget {
  const Home({super.key});

  @override
  State<Home> createState() => _HomeState();
}

class _HomeState extends State<Home> {
  final PageController _pageController = PageController();
  final _worldListKey = GlobalKey<WorldListState>();

  int _selectedPage = 0;
  bool _didInitialSessionCheck = false;

  static const int _nowTabIndex = 2;

  List<Widget> _buildAppBars(BuildContext context, Session? currentSession) {
    final base = [
      const FriendsListAppBar(),
      const SessionListAppBar(),
      WorldListAppBar(
        onQueryChanged: ({required needle, required sortDirection, required sortParameter}) {
          _worldListKey.currentState?.changeQuery(
            needle: needle,
            sortDirection: sortDirection,
            sortParameter: sortParameter,
          );
        },
      ),
      const InventoryBrowserAppBar(),
      const SettingsAppBar(),
    ];
    if (currentSession != null) {
      return [
        base[0],
        base[1],
        AppBar(
          title: const Text("In world"),
          scrolledUnderElevation: 0,
        ),
        base[2],
        base[3],
        base[4],
      ];
    }
    return base;
  }

  List<Widget> _buildPages(Session? currentSession) {
    final base = [
      const FriendsList(),
      const SessionList(),
      WorldList(key: _worldListKey),
      const InventoryBrowser(),
    ];
    if (currentSession != null) {
      return [
        base[0],
        base[1],
        InWorldNowPage(session: currentSession),
        base[2],
        base[3],
      ];
    }
    return base;
  }

  List<NavigationDestination> _buildDestinations(Session? currentSession) {
    const base = [
      NavigationDestination(icon: Icon(Icons.message), label: "Chat"),
      NavigationDestination(icon: Icon(Icons.groups), label: "Sessions"),
      NavigationDestination(icon: Icon(Icons.public), label: "Worlds"),
      NavigationDestination(icon: Icon(Icons.inventory), label: "Inventory"),
    ];
    if (currentSession != null) {
      return [
        base[0],
        base[1],
        const NavigationDestination(icon: Icon(Icons.videogame_asset), label: "Now"),
        base[2],
        base[3],
      ];
    }
    return base;
  }

  @override
  Widget build(BuildContext context) {
    return Consumer<SessionClient>(
      builder: (context, sessionClient, _) {
        if (!_didInitialSessionCheck) {
          _didInitialSessionCheck = true;
          WidgetsBinding.instance.addPostFrameCallback((_) {
            sessionClient.refreshCurrentSession();
          });
        }
        final currentSession = sessionClient.currentSession;
        final appBars = _buildAppBars(context, currentSession);
        final pages = _buildPages(currentSession);
        final destinations = _buildDestinations(currentSession);
        var selectedPage = _selectedPage;
        if (currentSession == null && selectedPage >= _nowTabIndex) {
          selectedPage = selectedPage - 1;
          if (selectedPage < 0) selectedPage = 0;
          WidgetsBinding.instance.addPostFrameCallback((_) {
            if (mounted) {
              _pageController.jumpToPage(selectedPage);
              setState(() => _selectedPage = selectedPage);
            }
          });
        }
        return Scaffold(
          backgroundColor: Theme.of(context).colorScheme.surface,
          appBar: PreferredSize(
            preferredSize: const Size.fromHeight(kToolbarHeight),
            child: AnimatedSwitcher(
              duration: const Duration(milliseconds: 200),
              child: KeyedSubtree(
                key: ValueKey<int>(selectedPage),
                child: appBars[selectedPage],
              ),
            ),
          ),
          body: PageView(
            controller: _pageController,
            physics: const NeverScrollableScrollPhysics(),
            children: pages,
          ),
          bottomNavigationBar: NavigationBar(
            selectedIndex: selectedPage,
            onDestinationSelected: (index) {
              _pageController.animateToPage(
                index,
                duration: const Duration(milliseconds: 200),
                curve: Curves.easeOut,
              );
              setState(() {
                _selectedPage = index;
              });
            },
            destinations: destinations,
          ),
        );
      },
    );
  }
}
